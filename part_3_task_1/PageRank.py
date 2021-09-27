import sys
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower
from pyspark.sql.types import StringType, StructField, StructType


start = perf_counter()
spark = (
    SparkSession
        .builder
        .appName('pageRank')
        .config('spark.driver.memory', '30g')
        .config('spark.executor.memory', '30g')
        .config('spark.executor.cores', '5')
        .config('spark.task.cpus', '1')
        .getOrCreate()
)

schema = StructType([
    StructField('FromNodeID', StringType(), True),
    StructField('ToNodeID', StringType(), True)
])


# Read args
input_file = sys.argv[1].rstrip()
output_file = sys.argv[2].rstrip()

# Read in and format `df`
df = (
    spark.read
         .options(header=False,
                  delimiter='\t',
                  schema=schema)
         .csv(input_file)
)
df = (
    df.withColumnRenamed('_c0', 'FromNodeID')
      .withColumnRenamed('_c1', 'ToNodeID')
      .withColumn('FromNodeID', lower(col('FromNodeID')))
      .withColumn('ToNodeID', lower(col('ToNodeID')))
)

# Set initial ranks
# NOTE: Neighbor # is pre-calculated for speed
node_counts = (
    df.groupby('FromNodeID')
      .count()
      .withColumnRenamed('count', 'neighbors')
      #.persist(StorageLevel.MEMORY_AND_DISK)
)
entries = (
    df.join(node_counts, df.FromNodeID == node_counts.FromNodeID)
      .select(df.FromNodeID, df.ToNodeID, node_counts.neighbors)
)
ranks = (
    node_counts
        .select('FromNodeID')
        .withColumnRenamed('FromNodeID', 'ID')
        .withColumn('ranks', lit(1))
)

# Run Page Rank
for i in range(10):
    # Calculate contributions
    contributions = (
        entries.join(ranks, entries.FromNodeID == ranks.ID, 'left_outer')
               .fillna(.15, subset='ranks')
    )
    contributions = (
        contributions
               .withColumn('contributions', contributions.ranks / contributions.neighbors)
               .select('ToNodeID', 'contributions', 'neighbors')
               .withColumnRenamed('ToNodeID', 'ID')
               .groupby('ID', 'neighbors')
               .sum()
               .withColumnRenamed('SUM(contributions)', 'contributions')
    )

    # Calculate new ranks
    ranks = (
        contributions
            .withColumn('ranks', 0.15 + 0.85 * contributions.contributions)
            .select('ID', 'ranks')
    )

# End timing
end = perf_counter()
print('Total Time:', end - start)

# Force computation
ranks.show()
if len(output_file) > 0:
    ranks.write.csv(output_file)

#import time; time.sleep(100)
