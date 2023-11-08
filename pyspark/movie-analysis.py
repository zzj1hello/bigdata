import time

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

'''
1. 查询用户打的平均分
2. 查询电影平均分
3. 查询大于平均分的电影的数量
4. 查询高分电影中(>3) 打分次数最多的用户，此人打的平均分是多少
5. 查询每个用户的平均打分、最低打分、最高打分
6. 查询被评分超过100次的电影 的平均分 取Top100
'''
spark = SparkSession.builder.appName('movie analysis').master('local[*]').getOrCreate()
sc = spark.sparkContext

# 四列分别代表 用户ID 电影ID 评分 时间戳
schema = StructType().add("user_id", StringType(), nullable=True)\
                     .add("movie_id", StringType(), nullable=True)\
                     .add("rank", IntegerType(), nullable=True)\
                     .add("ts", StringType(), nullable=True)
df = spark.read.format("csv").option("sep", "\t").schema(schema=schema).load('测试数据/sql/u.data')
df.printSchema()
df.explain(mode="formatted")
df.show(5)

# 1. 查询用户打的平均分
df.groupby(df['user_id']).avg('rank').withColumnRenamed('avg(rank)', 'avg_user').\
            withColumn('avg_user', F.round('avg_user', 2)).orderBy("avg_user", ascending=False).show(5)

# 2. 查询电影平均分
df.createTempView("rank_table")
spark.sql("select movie_id, round(avg(rank), 2) as avg_movie from rank_table group by movie_id order by avg_movie desc limit 5").show()

# 3. 查询大于平均分的电影的数量
avg_val = df.select(F.avg(df['rank'])).first()['avg(rank)']
print("电影平均分：", avg_val)
print("大于平均分的电影数量：", df.where(df['rank'] > avg_val).count())
sql_res = spark.sql("""
                select count(*) as cnt from rank_table where rank > (
                        select avg(rank) from rank_table
                    )
            """).first()['cnt']
print("SQL风格结果", sql_res)

# 4. 查询高分电影中(>3) 打分次数最多的用户，此人打的平均分是多少

## 使用窗口函数 导致Moving all data to a single partition 会产生性能下降
spark.sql("""
    select round(avg(rank), 2) from rank_table where user_id=(
        select user_id from (
            select user_id, count(*) as cnt, max(count(*)) over () as max_cnt
            from rank_table where rank > 3  group by user_id
        ) where cnt = max_cnt
    )
""").show()
# max() over的窗口函数可以用order by+limit 1优化
spark.sql("""
    select round(avg(rank), 2) from rank_table where user_id=(
        select user_id from (
            select user_id, count(*) as cnt
            from rank_table where rank > 3  group by user_id order by cnt desc limit 1
        )
    )
""").show()

df_count = df.where(df['rank']>3).groupby('user_id').count().orderBy("count", ascending=False)
df_count.show(5)
user_ans = df_count.limit(1).first()['user_id']
df.where(df['user_id']==user_ans).select(F.round(F.avg('rank'), 2)).show()

# 5. 查询每个用户的平均打分、最低打分、最高打分
spark.sql("""
    select user_id, round(avg(rank), 2) avg_rank, min(rank) min_rank, max(rank) max_rank from rank_table group by user_id
""").show(5)

df.groupby('user_id').agg(F.round(F.avg('rank'), 2).alias("avg_rank"),
                                  F.min("rank").alias("min_rank"),
                                  F.max("rank").alias("max_rank")).show(5)

# 6. 查询被评分超过100次的电影 的平均分 取Top10
spark.sql("""
    select movie_id, count(rank) cnt, round(avg(rank), 2) avg_rank from rank_table group by movie_id having count(rank) > 100 order by avg_rank desc limit 10
""").show()
df_movie_agg = df.groupby("movie_id").agg(F.count("movie_id").alias('cnt'), F.round(F.avg('rank'), 2).alias('avg_rank'))
df_movie_agg.where(df_movie_agg['cnt'] > 100).orderBy('avg_rank', ascending=False).limit(10).show()

# time.sleep(1000)