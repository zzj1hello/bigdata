离线数据处理⭐️⭐️⭐️⭐️⭐️

主要围绕Hive，Spark的学习
Hive的基本架构以及工作流程
Hive的基本调优
Spark的基本架构以及工作流程
Spark的基本调优
数据倾斜的处理



Hive： https://www.bilibili.com/video/BV1BS4y1w7FR/?p=2&share_source=copy_web&vd_source=b0c3cad8671d1f2fa75d01a0a18e195c

马士兵Spark：https://www.bilibili.com/video/BV1Tg411Y7pr?p=1&vd_source=89ddf71eb38188bf588f77ea08dd93b4

电信用户行为分析： https://www.bilibili.com/video/BV1L24y1o7f7/?share_source=copy_web&vd_source=b0c3cad8671d1f2fa75d01a0a18e195c

在线教育项目：https://www.bilibili.com/video/BV1ef4y1B7KX?p=40&vd_source=89ddf71eb38188bf588f77ea08dd93b4



# Hive

本质是作为一个将SQL转换成MapReduce的任务进行运算的客户端工具，MR再从HDFS中返回SQL查询结果

- 字段元数据存在数据库中
- 查询语言称为HQL，与SQL非常类似

![image-20230901213429386](assets/image-20230901213429386.png)

优点：

1. 免去写MR，开发效率提高
2. 适合处理大数据

缺点：

1. 执行延迟比较高，小数据处理，不如直接在MySQL中存数据，查数据
2. SQL没法处理While，Hive也没法处理迭代式计算
3. 能转成MR的能力有限
4. 粒度较粗，调优比较困难