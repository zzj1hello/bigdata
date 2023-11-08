import string

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('udf').master('local[3]').getOrCreate()
sc = spark.sparkContext

def func(num):
    # 对某列的每个元素num进行处理
    return num * 10

rdd = sc.parallelize(range(5)).map(lambda x: [x])
print(rdd.collect())
df = rdd.toDF(['num'])

udf_1 = spark.udf.register('udf1', func, IntegerType())
df.selectExpr("udf1(num)").show()  # SQL风格：对num列施加函数udf1
df.select(udf_1(df['num'])).show() # DSL风格：使用udf_1 UDF变量

# 只用于DFL风格的UDF，只返回UDF对象变量
udf_2 = F.udf(func, IntegerType())
df.select(udf_2('num')).show()

rdd = sc.textFile('测试数据/words.txt').map(lambda x: [x])
df = rdd.toDF(['word'])

# 返回ArrayType(StringType())类型的UDF，得到类似F.split()的效果
def func2(word):
    return word.split()
udf_3 = spark.udf.register('udf3', func2, ArrayType(StringType()))
df.select(udf_3('word')).show()
df.createTempView("word_tbl")
# 等价于df.selectExpr("udf3(word)").show()
spark.sql("select udf3(word) from word_tbl").show()

# 类似F.split()的结果
df.withColumn('word', F.explode(udf_3('word'))).show()


# 返回python字典 需要指定StructType()类型
rdd = sc.parallelize([[1], [2], [3]])
df = rdd.toDF(['num'])

def fun3(num):
    return {'num': num, 'letter': string.ascii_letters[num]}
udf_4 = F.udf(fun3, StructType().add('num', IntegerType(), nullable=False).add('letter', StringType(), nullable=False))
df = df.select(udf_4(df['num']).alias('udf_res'))
df.select("udf_res.num", "udf_res.letter").show()

## pyspark的UDAF 只能转换为RDD算子实现
rdd = sc.parallelize(range(5), 3).map(lambda x: [x])
df = rdd.toDF(['num'])
# 使用mapPartitions 实现的UDAF 需要输入RDD分区数为1 因为是要得到所有记录的聚合结果
rdd_one_part = df.rdd.repartition(1)
def agg_sum(iter):
    sum = 0
    for row in iter:
        print(row)
        sum += row['num']
    return [sum]
print(rdd_one_part.mapPartitions(agg_sum).collect())
# 转为一维RDD直接reduce聚合
print(rdd.flatMap(lambda x: x).reduce(lambda a, b: a+b))