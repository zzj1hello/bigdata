from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('movie analysis').master('local[*]').getOrCreate()
sc = spark.sparkContext

df = spark.read.format('csv').schema("sid STRING, cname STRING, score INTEGER").load('测试数据/sql/stu_score_2.txt')
df.printSchema()
df.show(5)

df.createTempView('score_tbl')
spark.sql("""
    select *, rank() over(order by score desc) as rank, 
            dense_rank() over(partition by cname order by score desc) as dense_rank, 
            row_number() over(partition by cname order by score desc) as row_rank  from score_tbl 
""").show()
spark.sql("""
    select *, ntile(4) over(partition by cname order by score desc)  as bucket  from score_tbl
""").show()
