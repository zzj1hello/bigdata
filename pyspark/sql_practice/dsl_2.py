import collections

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

import findspark
findspark.init()
import time


spark = SparkSession.builder.master('local[*]').appName('dsl-query')\
            .config("spark.sql.warehouse.dir", 'hdfs://localhost:9000/user/hive/warehouse')\
            .config("hive.metastore.uris", 'thrift://localhost:9083')\
            .config("spark.sql.windowExec.buffer.spill.threshold", '1000000')\
            .enableHiveSupport().getOrCreate()

sc = spark.sparkContext

# start = time.time()
order_info = spark.read.table("sql_practice.order_info")
start = time.time()
# pyspark的UDAF要.repartition(1) 还是牺牲了并行度
order_info_rdd = order_info.select(['user_id', 'create_date']).rdd.map(lambda x: (x[0], x[1])).repartition(1)
# .map(lambda x: (x[0], x[1])).partitionBy(10, lambda x: x[0]).take(5)
import datetime
# !pip install sortedcontainers
# from sortedcontainers import SortedList
import bisect

def group_agg(itr):
    dic = collections.defaultdict(set)
    for user_id, date in itr:
        cur_date = datetime.datetime.strptime(date, "%Y-%m-%d").date() # %Y匹配2023四位年份 %y匹配23两位年份
        dic[user_id].add(cur_date)

    ''' 没必要有序再查询
        k = bisect.insort_left(a=res[user_id], x=cur_date)
        if k==len(res[user_id]):
            res[user_id].append(cur_date)
        else:
            if res[user_id][k+1] == cur_date:
                continue
            else:
                res[k].append(cur_date)
    '''
    # 遍历每个user_id的下单日期 计算连续子序列 当出现连续长度超过3 返回user_id
    ## 力扣第128题 https://leetcode.cn/problems/longest-consecutive-sequence/description/
    res = []
    for user_id, dates in dic.items():
        print(user_id, len(dates))
        # 记录每个date的开始和结束
        tmp = collections.defaultdict(int)
        for date in dates:

            l, r = tmp[date-datetime.timedelta(days=1)], tmp[date+datetime.timedelta(days=1)]
            tmp[date] = 1+l+r
            tmp[date+datetime.timedelta(days=r)] = tmp[date]
            tmp[date-datetime.timedelta(days=l)] = tmp[date]

            if tmp[date] >= 3:
                res.append(user_id)
                break
        # print(tmp)
    return res

# pyspark 无论什么ide都没法在rdd的自定义函数里头debug 可以采样先试试函数能不能行
# lst = order_info_rdd.take(20)
# print(group_agg(lst))
# print(order_info_rdd.mapPartitions(group_agg).collect())

end = time.time()
print(f"DSL-mine耗时：{round((end-start), 3)}")

start = time.time()
window = Window.partitionBy('user_id').orderBy('create_date')
order_rk = order_info.groupBy(['user_id', 'create_date']).agg(F.rank().over(window).alias('flag'))
order_res = order_rk.withColumn('flag', F.date_sub(order_rk.create_date, order_rk.flag))
order_res = order_res.groupBy(['user_id', 'flag']).count().where('count(flag)>=3').select('user_id').distinct()
order_res.show()

end = time.time()
print(f"DSL-standard耗时：{round((end-start), 3)}")

start = time.time()
# order_info.createTempView('order_info')
spark.sql("""
    select distinct user_id
    from(
    
          select user_id
               , create_date
                -- 关键的flag计算：使用date_sub(日期, 减去的差值) 得到的flag相同表示连续 再按照flag分组判断个数是否满足条件即可
               , date_sub(create_date, row_number() over (partition by user_id order by create_date)) flag
          from (
                   select user_id
                        , create_date
                   from sql_practice.order_info -- sql_practice.
                   group by user_id, create_date -- groupby会自动去重（相同分组号）因此后续使用三种不同的rank都一样
               ) 
    
        )
    group by user_id, flag
    having count(flag) >= 3
""").show()
end = time.time()
print(f"SQL耗时：{round((end-start), 3)}")

''' 结论 
DSL-mine的算法复杂度O(n) SQL的算法复杂度O(n)  尽管算法设计的更好，但DSL没有SQL来得快
- 先从表中查数据+DSL-mine：4.654s
- 直接DSL-mine：1.152s
- 直接DSL-standard：3.017s
- 先从表中查数据+再创建视图+SQL：4.654-1.152+0.478=3.98s
- hive元数据+sql风格：0.547s
- sparksql console：2.657s
- hivesql console (mapreduce): 43.625s
- bin/spark-sql -f 2.sql: 5.17s
'''