import collections

from pyspark.sql import SparkSession
import findspark
findspark.init()
import time


spark = SparkSession.builder.master('local[*]').appName('dsl-query')\
            .config("spark.sql.warehouse.dir", 'hdfs://localhost:9000/user/hive/warehouse')\
            .config("hive.metastore.uris", 'thrift://localhost:9083')\
            .config("spark.sql.windowExec.buffer.spill.threshold", '1000000')\
            .enableHiveSupport().getOrCreate()

sc = spark.sparkContext

start = time.time()
order_info = spark.read.table("sql_practice.order_info")
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
print(order_info_rdd.mapPartitions(group_agg).collect())

end = time.time()
print(f"DSL耗时：{round((end-start), 2)}")


start = time.time()
# order_info.createTempView('order_info')
spark.sql("""
    select distinct user_id
    from (
             select user_id
             from (
                      select user_id
                           , create_date
                           , date_sub(create_date, row_number() over (partition by user_id order by create_date)) flag
                      from (
                               select user_id
                                    , create_date
                               from sql_practice.order_info
                               group by user_id, create_date
                           ) t1 -- 同一天可能多个用户下单，进行去重
                  ) t2 -- 判断一串日期是否连续：若连续，用这个日期减去它的排名，会得到一个相同的结果
             group by user_id, flag
             having count(flag) >= 3 -- 连续下单大于等于三天
         ) t3;
""").show()
end = time.time()
print(f"SQL耗时：{round((end-start), 2)}")

''' 结论
还是spark的SQL风格更快 估计还是受UDAF并行度的限制
- 先从表中查数据+DSL风格：8.82s
- 直接DSL风格：4.56s
- 先从表中查数据+再创建视图+SQL：8.82-4.56+2.53=6.79s
- hive元数据+sql风格：2.24s
- sparksql console：2.76s
- hivesql console (mapreduce): 56.56s
'''