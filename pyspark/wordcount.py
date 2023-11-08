from pyspark import SparkConf, SparkContext
import findspark
findspark.init()
import os
os.environ['HADOOP_CONF_DIR'] = '/home/ubuntu/hadoop/etc/hadoop'
master = 'yarn' # 指定local模式local[*]  spark://localhost:7077  yarn
appName = 'WordCountHelloWorld'
conf = SparkConf().setAppName(appName).setMaster(master)
# 设置了 findspark.init() 不需要再设置python环境
# conf.set("spark.pyspark.python", '/home/ubuntu/anaconda3/envs/pyspark/bin/python')
sc = SparkContext(conf=conf)


# 读取HDFS文件（保证各机器都能访问） 也可读取本地文件（pycharm只连接解释器，本地数据会静默上传到服务器） 服务器文件 
# file = '测试数据/words.txt'
file = 'hdfs://localhost:9000/data/wc/input/data.txt'
file_rdd = sc.textFile(file) 

# 将单词进行切割, 得到一个存储全部单词的集合对象
words_rdd = file_rdd.flatMap(lambda line: line.split(" "))

# 将单词转换为元组对象, key是单词, value是数字1
words_with_one_rdd = words_rdd.map(lambda x: (x, 1))

# 将元组的value 按照key来分组, 对所有的value执行聚合操作(相加)
result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

# 通过collect方法收集RDD的数据打印输出结果
print(result_rdd.collect())

