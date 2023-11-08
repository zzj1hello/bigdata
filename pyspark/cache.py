from pyspark import SparkConf, SparkContext
import time
from pyspark.storagelevel import StorageLevel

master = 'local[*]' # 指定local模式local[*]
appName = 'test_cache'
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

# sc = SparkContext(master='local[*]', appName='test')

def test(is_cache=False):
    rdd1 = sc.textFile('测试数据/words.txt')
    rdd2 = rdd1.flatMap(lambda x: x.split(' '))
    rdd3 = rdd2.map(lambda x: (x, 1))

    if is_cache:
        rdd3.cache()
        # rdd3.persist()
        # rdd3.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
        
    # rdd3使用 reduceByKey() 聚合词频
    rdd4 = rdd3.reduceByKey(lambda x, y: x+y)
    print(rdd4.collect())

    # rdd3使用 groupByKey() 聚合词频
    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    if is_cache:
        rdd3.unpersist()

    time.sleep(1000)

if __name__ == '__main__':
    test(True)