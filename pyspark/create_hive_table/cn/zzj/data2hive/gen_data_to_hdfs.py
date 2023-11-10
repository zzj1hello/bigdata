#!/usr/bin/env python
# @desc : 使用pyspark生成数据记录到HDFS中
__coding__ = "utf-8"
__author__ = "zzj"

from pyspark.sql import SparkSession
from create_hive_table.cn.zzj.utils import ConfigLoader         # 导入配置文件解析包
import findspark
findspark.init()

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("write to HDFS") \
    .getOrCreate()

for i in
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])

    # 将数据写入 HDFS
    df.write.csv("hdfs://localhost:9000/path/to/output.csv")
    # 显示数据
    df.show()
