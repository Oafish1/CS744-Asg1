from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pyspark.sql.functions as f
import sys
import time
​
spark = (SparkSession
        .builder
        .appName("PageRank")
        .getOrCreate())
start = time.time()
​
#We have manually deleted the top few lines of web-BerkStan.txt file
#df = spark.read.options(header = False, delimiter = '\t').csv('/proj/uwmadison744-f21-PG0/data-part3/enwiki-pages-articles/link*')
#df = spark.read.options(header = False, delimiter = '\t').csv('/mnt/data/hadoop-3.2.2/data/namenode/enwiki-pages-articles/link-enwiki-20180601-pages-articles10.xml-p2336425p3046511')
#df.where(df._c0== 'Bralos').show()
df = spark.read.options(header = False, delimiter = '\t').csv('/proj/uwmadison744-f21-PG0/data-part3/enwiki-pages-articles/link*')
​
​
df = df.withColumnRenamed("_c0","FromNode").withColumnRenamed("_c1","ToNode")#.repartition("FromNode")
df = df.withColumn("FromNode",f.lower(f.col("FromNode"))).withColumn("ToNode", f.lower(f.col("ToNode")))
#get number of outgoing connections for each node
count = df.groupby("FromNode").count().withColumnRenamed("count","num_neighbors").withColumnRenamed("FromNode","CountId").repartition("CountId")#.persist()
​
#get list of unique nodes and initialize rank to 1
node_ids1 = df.select('FromNode').withColumnRenamed("FromNode", "NodeId")
#node_ids2 = df.select('ToNode').withColumnRenamed("ToNode","NodeId")
#nodes = (node_ids1.union(node_ids2)).select("NodeId").distinct()
ranks = node_ids1.withColumn('rank',lit(1))#.repartition("NodeId")
​
count_df = count.join(df,count.CountId == df.FromNode, 'inner')#.repartition("FromNode")
​
​
for _ in range(10):
    #join rank, count and df dataframes
    #rank_count_df = ranks.join(count,ranks.NodeId == count.FromNodeId, 'inner').repartition("FromNodeId").join(df, count.FromNodeId == df.FromNodeId, 'inner')\
     #       .repartition("ToNodeId")
    #rank_count_df = ranks.join(count, ranks.NodeId == count.FromNode, 'inner').join(df, count.FromNode == df.FromNode, 'inner')
    
    #calculate contribution from neighbors
    #rank_count_df = rank_count_df.withColumn('Add', rank_count_df.rank/rank_count_df.num_neighbors)
    rank_count_df = count_df.join(ranks, ranks.NodeId == count_df.FromNode, 'inner')
​
    #calculate contribution from neighbors
    rank_count_df = rank_count_df.withColumn('Add', rank_count_df.rank/rank_count_df.num_neighbors)
    #sum contributions grouped by ToNode
    ranks = rank_count_df.groupBy("ToNode").agg({'Add':'sum'}).withColumnRenamed("ToNode","NodeId").withColumnRenamed("sum(Add)","rank")
    ranks = ranks.withColumn("rank", 0.15 + 0.85*ranks.rank)#.repartition("NodeId")
​
#count = ranks.count()
ranks.write.csv('hdfs://10.10.1.1:9000/data/output/wiki_output')
end = time.time()
print("\n Total time: " + str(end-start) + "\n")
#print("\n Count:" + str(df.count()) + "\n")
