from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.master('local[*]').appName('dsl-query')\
            .config("spark.sql.warehouse.dir", 'hdfs://localhost:9000/user/hive/warehouse')\
            .config("hive.metastore.uris", 'thrift://localhost:9083')\
            .config("spark.sql.windowExec.buffer.spill.threshold", '1000000')\
            .enableHiveSupport().getOrCreate()

sc = spark.sparkContext

df_order_detail = spark.read.table("sql_practice.order_detail")
df_info = spark.read.table("sql_practice.sku_info")
df_order_detail.show(5)

df_rank = df_order_detail.groupBy('sku_id', 'price').agg(F.sum('sku_num').alias('order_num'))\
        .withColumn("rk", F.rank().over(Window.orderBy(F.desc('order_num'))))
df_rank = df_rank.withColumn('total_sales', df_rank.price*df_rank.order_num)
df_rank.show(5)
df_rank = df_rank.where('rk=2')
# print(df_rank.take(2)[-1]['sku_id'])

# 无法直接降序+limit(2) 因为可能第二位的不止一个
# df_no_rank = df_order_detail.groupBy('sku_id', 'price').agg(F.sum('sku_num').alias('order_num')).orderBy('order_num')
# df_no_rank = df_no_rank.withColumn('total_sales', df_no_rank.price*df_no_rank.order_num)
# rank2_sku = df_no_rank.take(2)[-1]['sku_id']
# print(df_no_rank.take(2)[-1]['sku_id'])

df_join = df_info.join(df_rank, ['sku_id']).select(['name', 'total_sales'])
df_join.show()