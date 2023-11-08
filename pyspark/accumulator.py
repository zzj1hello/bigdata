from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('accumulator').setMaster('local[*]')
sc = SparkContext(conf=conf)

file_rdd = sc.parallelize(range(8), 2)
cnt_raw = 0
cnt_acm = sc.accumulator(0)

def map_func(data):
    global cnt_raw, cnt_acm
    cnt_raw += 1
    cnt_acm += 1
    print(cnt_raw, cnt_acm)

file_rdd.foreach(map_func)
print(cnt_raw, cnt_acm) 