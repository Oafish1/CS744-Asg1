import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructType, StructField
#from pyspark.sql.functions import lower, col
import pyspark.sql.functions as F
from pyspark import StorageLevel
import time

#start = time.time()
spark = (SparkSession
                    .builder
                    .appName('pageRank2')
                    .config('spark.driver.meory', '30g')
                    .config('spark.executor.meory', '30g')
                    .config('spark.executor.cores', '5')
                    .config('spark.task.cpus', '1')
                    .getOrCreate())

#df = spark.read.options(header = True, delimiter = '\t').csv('hdfs://10.10.1.1:9000/data/web-BerkStan.txt')

inputPath = sys.argv[1].rstrip()

outputPath = sys.argv[2].rstrip()

# read the file with schema
schema = StructType([
        StructField('FromNodeID', StringType(), True),
        StructField('ToNodeID', StringType(), True)])

df = spark.read.options(header = False, delimiter = '\t', schema = schema).csv(inputPath)
df = df.withColumnRenamed('_c0', 'FromNodeId').withColumnRenamed('_c1', 'ToNodeId')

# lower the case
df = df.withColumn('FromNodeId', F.lower(F.col('FromNodeId'))).withColumn('ToNodeId', F.lower(F.col('ToNodeId')))


# Construct a table contain how much neighbors each node has
nodes_count = df.groupby("FromNodeId").count().withColumnRenamed("count", 'neighbors').distinct().repartition("FromNodeId")
# persist the table
nodes_count = nodes_count.persist(StorageLevel.MEMORY_AND_DISK)
#rank = nodes_count.select("FromNodeId").withColumnRenamed("FromNodeId", 'ID').withColumn("rank",lit(1))
rank = 1

for i in range(10):
    # Calculate contributions

    #  add a new attribute, rank
    nodes_count = nodes_count.withColumn('rank', rank / nodes_count.neighbors)
    # calculate the sum of all contributions
    rank = nodes_count.select(F.sum('rank')).collect()[0][0] * 0.85 + 0.15
    # reset the nodes_count table
    nodes_count = nodes_count.select('FromNodeId', 'neighbors').repartition('FromNodeId')


nodes_count = nodes_count.select('FromNodeId').withColumn('rank', lit(rank))
#end = time.time()
#rank.write.csv("res.csv")
#print("time :" + str(end - start))

nodes_count.write.csv(outputPath)
nodes_count.show()