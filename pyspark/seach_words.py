from operator import add
from pyspark import SparkConf, SparkContext, StorageLevel
import jieba
# from sw_dependency import *

appname = 'search_words'
master =  'spark://localhost:7077' # 'local[*]'
conf = SparkConf().setAppName(appname).setMaster(master)
# conf.set("spark.driver.extraJavaOptions", "-Xdebug") # pycharm vsc都无法断点调试  
sc = SparkContext(conf=conf)
# sc.addPyFile('jieba.zip')

'''
分组聚合排序是最常用的操作，
开发时，对于map转换数据，可以先takeSample()或foreach(map函数) 可以了注释掉 继续写 
'''
rdd_file = sc.textFile('测试数据/SogouQ.txt')
split_rdd = rdd_file.map(lambda x: x.split('\t'))
# 持久化存储文件读取出来的rdd 用以多次使用 否则动作输出完就没有
split_rdd.persist(storageLevel=StorageLevel.DISK_ONLY)
# print(rdd_file.takeSample(False, 3))

# sw_rdd = split_rdd.map(lambda x: (x[2], 1))
# rdd_res = sw_rdd.reduceByKey(lambda a, b: a+b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)

def extract_kw(sent):
    res = []
    for kw in jieba.lcut(sent):
        if kw in ['谷', '帮', '客']:
            continue
        if kw =='传智播': kw = "传智播客"
        if kw == '院校': kw = '院校帮'
        if kw == '博学': kw = '博学谷'
        res.append((kw, 1))
    return res

sw_rdd = split_rdd.map(lambda x: x[2])
# debug_data = sw_rdd.takeSample(False, 10, 1)
# for data in debug_data:
#     print(extract_kw(data))
kw_rdd = sw_rdd.flatMap(extract_kw)
res1 = kw_rdd.reduceByKey(lambda a, b: a+b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
print("全网Top5高频搜索词: ", res1)


def extract_user_kw(data):
    res = []
    user_id, user_kw = data[1], extract_kw(data[2])
    for kw in user_kw:
        res.append((user_id + "-" + kw[0], 1))
    return res 

# for debug in split_rdd.takeSample(False, 5):
#     print(extract_user_kw(debug))
user_kw_rdd = split_rdd.flatMap(extract_user_kw)
res2 = user_kw_rdd.reduceByKey(lambda a, b: a+b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)

# 无法简单通过分组.groupBy(lambda a: a.split('_')[0]).map(lambda x: (x[0], list(x[1])))得到分组top5
print("带有用户id的Top5高频搜索词: ", res2)

''' SparkSQL 窗口函数实现的分组排序
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# 创建SparkSession
spark = SparkSession.builder.getOrCreate()

# 创建示例数据
data = [("A", 1), ("A", 2), ("A", 3), ("B", 4), ("B", 5), ("C", 6), ("C", 7)]
df = spark.createDataFrame(data, ["group", "value"])

# 定义窗口规范
window_spec = Window.partitionBy("group").orderBy(df["value"].desc())

# 添加排序序号列
df_with_rank = df.withColumn("rank", row_number().over(window_spec))

# 选择Top-N记录
n = 2
top_n_df = df_with_rank.filter(df_with_rank["rank"] <= n)

# 显示结果
top_n_df.show()
'''

## 热门搜索时间段
 
hour_rdd = split_rdd.map(lambda x: (x[0].split(':')[0], 1))
res3 = hour_rdd.reduceByKey(add).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
print('Top5热门搜索: ' , res3)