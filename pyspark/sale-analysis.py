from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel
'''
需求1: 各省销售额的统计
需求2: TOP3销售省份中，有多少店铺达到过日销售额1000+
需求3: TOP3省份中，各省的平均订单价格
需求4: TOP3省份中，各个省份的支付类型比例

receivable: 订单金额
storeProvince：店铺省份
dateTS： 订单的销售日期
payType： 支付类型
storeID: 店铺ID
'''
spark = SparkSession.builder.appName('movie analysis').master('local[*]').getOrCreate()
sc = spark.sparkContext

df = spark.read.format('json').load('测试数据/mini.json')\
                        .select('storeProvince', 'storeID', 'receivable', 'dateTS', 'payType')\
                        .dropna(thresh=1, subset=['storeProvince']).where('storeProvince != "null"')\
                        .where('receivable < 10000')
df.printSchema()
df.show(5)

## 需求1: 各省销售额的统计
df_1 = df.groupBy('storeProvince').agg(F.sum('receivable'))

df_1 = df_1.withColumnRenamed('sum(receivable)', 'total_money')\
            .withColumn('total_money', F.round('total_money', 2))\
            .orderBy('total_money', ascending=False)
df_1.show()
# df_1.write.format('jdbc').mode('overwrite').option('url', 'jdbc:mysql://113.31.116.219:3306/bigdata?usessl=false&useUnicode=true)')\
#                         .option('dbtable', 'province_sale').option('user', 'zzjian').option('password', 'Aa666666')\
#                         .option('encoding', 'utf-8').save()

## 需求2: TOP3销售省份中，有多少店铺达到过日销售额1000+
df_top3_province = df_1.limit(3).select('storeProvince') # .withColumnRenamed('storeProvince', 'top3_province')
df_join = df.join(df_top3_province, 'storeProvince')
df_join.persist(StorageLevel.MEMORY_AND_DISK) # 持久化保存该DF 其会在执行SparkSQL中

# F.unix_timestamp(df['dateTS'].substr(0, 10), 'yyyy-MM-dd')  # ms时间戳的时间转换
df_2 = df_join.groupBy(['storeProvince', 'storeID', 'dateTS']).agg(F.sum('receivable').alias('daily_money'))\
            .where('daily_money>1000').dropDuplicates(subset=['storeID']).groupBy(df.storeProvince)\
            .count()
df_2.show()
# df_2.write.format('jdbc').mode('overwrite').option('url', 'jdbc:mysql://113.31.116.219:3306/bigdata?usessl=false&useUnicode=true)')\
#                         .option('dbtable', 'province_hot_store_count').option('user', 'zzjian').option('password', 'Aa666666')\
#                         .option('encoding', 'utf-8').save()

## 需求3: TOP3省份中，各省的平均订单价格
df_3 = df_join.groupBy('storeProvince').agg(F.round(F.avg('receivable'), 2).alias('avg_money'))
df_3.show()
# df_3.write.format('jdbc').mode('overwrite').option('url', 'jdbc:mysql://113.31.116.219:3306/bigdata?usessl=false&useUnicode=true)')\
#                         .option('dbtable', 'province_hot_store_order_avg').option('user', 'zzjian').option('password', 'Aa666666')\
#                         .option('encoding', 'utf-8').save()

## 需求4: TOP3省份中，各个省份的支付类型比例

### 使用DSL风格 没有UDAF 不好对新列整列聚合计算
# df_4 = df_join.join(df_join.groupBy('storeProvince').agg(F.count('*').alias('total')), on='storeProvince', how='left')
# df_4 = df_4.groupby(['storeProvince', 'payType', 'total']).agg(F.count('*').alias('total_2'))
# df_4 = df_4.withColumn('percent', F.concat(F.round(df_4['total_2'] / df_4['total'] * 100, 2), F.lit('%')))\
#                 .select(['storeProvince', 'percent'])
# df_4.show()

### 使用SQL窗口函数 得到分组计数的新列
df_join.createTempView('province_pay_tbl')
def str_udf(num):
    return f'{(num*100):.2f}%'
spark.udf.register('str_udf', str_udf, StringType())
df_4 = spark.sql("""
    select storeProvince, str_udf(count('*') / total) as percent from
        (select storeProvince, payType, count('*') over(partition by storeProvince) as total from province_pay_tbl)
    group by storeProvince, payType, total
""")
df_4.show()
df_4.write.format('jdbc').mode('overwrite').option('url', 'jdbc:mysql://113.31.116.219:3306/bigdata?usessl=false&useUnicode=true)')\
                        .option('dbtable', 'province_hot_pay_percent').option('user', 'zzjian').option('password', 'Aa666666')\
                        .option('encoding', 'utf-8').save()

df_join.unpersist()