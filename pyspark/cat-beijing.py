from pyspark import SparkConf, SparkContext
import json

master = 'spark://10-24-17-107:7077'
appname = 'cat_beijing'
conf = SparkConf().setAppName(appname).setMaster(master)
sc = SparkContext(conf=conf)

file_rdd = sc.textFile("测试数据/order.text")
json_rdd = file_rdd.flatMap(lambda x: x.split("|"))
dict_rdd = json_rdd.map(lambda x: json.loads(x))
beijing_rdd = dict_rdd.filter(lambda x: x['areaName']=="北京")
cat_rdd = beijing_rdd.map(lambda x: f'北京_{x["category"]}').distinct()
print(cat_rdd.collect())