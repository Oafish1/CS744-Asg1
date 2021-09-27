import sys

from pyspark.sql import SparkSession


spark = (SparkSession
         .builder
         .appName('SimpleSort')
         .config('spark.executor.memory', '30g')
         .config('spark.executor.cores', '5')
         .config('spark.driver.memory', '30g')
         .config('spark.task.cpus', '1')
         .master('local')
         .getOrCreate()
)


# Typical HDFS Ports: 9870 8088
# Use same as in hadoop-3.2.2/etc/hadoop/core-site.xml
# Define Hadoop paths
hdfs_prefix = 'hdfs://10.10.1.1:9000/'
input_file = hdfs_prefix + sys.argv[1]
output_file = hdfs_prefix + sys.argv[2]

# Read and sort
df = spark.read.format('csv').option('header', True).load(input_file)
df = df.sort('cca2', 'timestamp')

# Combine (optional) and write
df = df.coalesce(1).repartition(1)
df.write.format('csv').option('header', True).save(output_file)
