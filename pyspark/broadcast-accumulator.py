from operator import add
import re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('bd-ac').setMaster('local[*]')
sc = SparkContext(conf=conf)

file_rdd = sc.textFile('测试数据/accumulator_broadcast_data.txt')
special_word = [',', '.', '!', '#', ',', '%']
def filter_map(data):
    if data != '':
        return re.split('\s+', data.strip()) # 按多个空格分割 
    else:
        return []
rdd_words = file_rdd.flatMap(filter_map)
print(rdd_words.collect())
special_word_list = sc.broadcast(special_word)
special_word_cnt = sc.accumulator(0)

def my_filter(data):
    global special_word_cnt
    if data in special_word_list.value:
        special_word_cnt += 1
        return False
    else:
        return True
rdd_res = rdd_words.filter(my_filter).map(lambda x: (x, 1)).reduceByKey(add)
print("正常单词统计结果：", rdd_res.collect())
print("特殊字符数为：", special_word_cnt)