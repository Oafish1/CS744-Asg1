import sys

from pyspark.sql import SparkSession


spark = (SparkSession
         .builder
         .appName('SimpleSort')
         .config('spark.executor.memory', '30g')
         .config('spark.driver.memory', '30g')
         .config('spark.executor.cores', '5')
         .config('spark.task.cpus', '1')
         .getOrCreate()
)


# Typical HDFS Ports: 9870 8088 9000
# Use same as in hadoop-3.2.2/etc/hadoop/core-site.xml
# Define Hadoop paths
input_file = sys.argv[1].rstrip()
output_file = sys.argv[2].rstrip()

# Read and sort
df = spark.read.format('csv').option('header', True).load(input_file)
df = df.sort('cca2', 'timestamp')

# Combine (optional) and write
df = df.coalesce(1).repartition(1)
df.write.format('csv').option('header', True).save(output_file)
