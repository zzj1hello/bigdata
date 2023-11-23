'''
从订单明细表（order_detail）中筛选出2021年总销量小于100的商品及其销量，并且不考虑上架时间小于一个月的商品
假设今天的日期是2022-01-10，
'''
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
# import findspark
# findspark.init()

spark = SparkSession.builder.master('local[*]').appName('dsl-query')\
            .config("spark.sql.warehouse.dir", 'hdfs://localhost:9000/user/hive/warehouse')\
            .config("hive.metastore.uris", 'thrift://localhost:9083')\
            .config("spark.sql.windowExec.buffer.spill.threshold", '1000000')\
            .enableHiveSupport().getOrCreate()

sc = spark.sparkContext

user_info = spark.read.table("sql_practice.user_info")
order_info = spark.read.table("sql_practice.order_info")

# 执行查询和转换
order_info.join(user_info, ['user_id'], 'inner') \
    .groupBy('create_date') \
    .agg(
        F.sum(F.when(user_info.gender == '男', order_info.total_amount).otherwise(0)).alias('total_amount_male'),
        F.sum(F.when(user_info.gender == '女', order_info.total_amount).otherwise(0)).alias('total_amount_female')
    ).show()


