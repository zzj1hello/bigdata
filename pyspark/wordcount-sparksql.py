from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
sc = spark.sparkContext

schema = StructType().add("word", StringType(), nullable=False)

# 使用RDD算子处理数据 再转换为DF和临时视图 使用SQL语句进行查询
rdd_file = sc.textFile('测试数据/words.txt')
rdd_data = rdd_file.flatMap(lambda x: x.split(' ')).map(lambda x: [x]) # 一定要转换为二维RDD
print(rdd_data.collect())
df_from_rdd = rdd_data.toDF(schema)
df_from_rdd.show()
df_from_rdd.createTempView("wordcount_tbl")
spark.sql('select word, count(*) as cnt from wordcount_tbl group by word order by cnt desc').show()

# 不使用RDD的算子 全部使用DF的算子
import pyspark.sql.functions as F
df = spark.read.format("text").schema(schema=schema).load("测试数据/words.txt") # 直接一行作为列元素
df.printSchema()
df.show()
# F.split()对指定列根据re进行字符串分割 ('\s'或' ')
# F.explode对分割得到的[] 每个值成一行组成列
# df,withColumn()指定列名 如果与该输入Column对象同名则替换 否则扩充一列
df_res = df.withColumn('word', F.explode(F.split(df['word'], '\s', -1)))
df_res.show()
df_res.groupby(df_res['word']).count().withColumnRenamed('count', 'cnt').orderBy("cnt", ascending=False).show()

