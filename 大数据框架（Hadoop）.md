Hadoop生态体系⭐️⭐️⭐️⭐️⭐️
主要掌握分布式数据处理，存储的思想，以及会写一些简单的MR任务：
Hadoop生态圈的全貌
MR,Yarn,HDFS的工作过程
Zookeeper架构以及原理

数据采集的对应框架Sqoop，Flume

任务平台Oozie和Azkaban，主要是了解工作原理



数据仓库：https://www.bilibili.com/video/BV1YK411f78Z?p=9&spm_id_from=pageDriver&vd_source=89ddf71eb38188bf588f77ea08dd93b4

黑马Hadoop实操：https://www.bilibili.com/video/BV1WY4y197g7/?spm_id_from=333.337.search-card.all.click&vd_source=89ddf71eb38188bf588f77ea08dd93b4

马士兵Hadoop+源码：https://www.bilibili.com/video/BV1HG411g71n?p=4&spm_id_from=pageDriver&vd_source=89ddf71eb38188bf588f77ea08dd93b4

数据采集技术：

- 离线数据采集：SQOOP、DataX、Kettle
- 实时数据采集：Flume、Maxwell、Canal、Nifi

中间件技术栈：

- 分布式协调服务zookeeper
- 分布式缓存Redis
- 分布式消息系统Kafka
- 分布式消息系统Pulsar
- ELK Stack数据分析

分布式存储技术：

- 分布式文件系统HDFS
- 分布式数据库HBase
- 分布式数据仓库Hive
- 数据湖Hudi
- 数据湖Delta lack
- 数据湖Iceberg

数据处理技术：

- 分布式计算框架MapReduce
- 分布式计算框架Spark
- 分布式计算框架Flink

OLAP生态：

- OLAP -Kylin 麒麟
- OLAP - Presto
- OLAP - Druid
- OLAP - Impala
- OLAP - ClickHouse
- OLAP - Phoenix
- OLAP - Kudu
- OLAP- Doirs

稳健架构设计

- 离线数仓构建方法论
- 实时数仓构建方法论
- 数据治理-数据质量管理
- 数据治理-元数据管理
- 数据治理-数据安全管理
- Kerberos
- 数据中台构建方法论
- 可视化：TCV，superset，Hue

集群调度体系

- 分布式资源调度Yarn
- 任务流调度oozie
- 任务流调度Azkaban
- Airflow
- 集群管理平台 cloudera Manager
- Ambari

数据挖掘算法。。



岗位：主要有三个， 需要掌握大数据核心技术+特定岗位的技能点

- 大数据ETL 开发
- 大数据离线数仓开发；大数据实时数仓开发
- 大数据接口开发
- 大数据业务分析开发
- 大数据平台开发；大数据可视化开发
- 大数据数据挖掘
- 总体难度，数仓<平台（JAVA）<挖掘

<img src="C:\Users\lenovo\Desktop\找工作\大数据开发\assets\image-20230723211553022.png" alt="image-20230723211553022"  />

![image-20230723211607724](C:\Users\lenovo\Desktop\找工作\大数据开发\assets\image-20230723211607724.png)

![image-20230723211615173](C:\Users\lenovo\Desktop\找工作\大数据开发\assets\image-20230723211615173.png)

![image-20230723211620603](C:\Users\lenovo\Desktop\找工作\大数据开发\assets\image-20230723211620603.png)



​    Hadoop（★★★★★）

​        Hadoop是由一个Apache基金会所开发的分布式系统基础架构，主要解决海量数据的存储和海量数据的分析计算问题，广义上来说，Hadoop通常是指一个更加广泛的概念--Hadoop生态圈。

​    Sqoop

​        Sqoop是一款开源的工具，主要用于在Hadoop、Hive与传统的数据库（MySql）间进行数据的传递，可以将一个关系型数据库（例如 ：MySQL，Oracle 等）中的数据导进到Hadoop的HDFS中，也可以将HDFS的数据导进到关系型数据库中。

​    Zookeeper

​        它是一个针对大型分布式系统的可靠协调系统，提供的功能包括：配置维护、名字服务、分布式同步、组服务等。

​    Hive（★★★★★）

​        Hive是基于Hadoop的一个数据仓库工具，可以将结构化的数据文件映射为一张数据库表，并提供简单的SQL查询功能，可以将SQL语句转换为MapReduce任务进行运行。

​    Flume

​        Flume是一个高可用的，高可靠的，分布式的海量日志采集、聚合和传输的系统，Flume支持在日志系统中定制各类数据发送方，用于收集数据；

​    Kafka（★★★★★）

​        Kafka是一种高吞吐量的分布式发布订阅消息系统；

​    HBase

​        HBase是一个分布式的、面向列的开源数据库。HBase不同于一般的关系数据库，它是一个适合于非结构化数据存储的数据库。

​    Spark（★★★★）

​        Spark是当前最流行的开源大数据内存计算框架。可以基于Hadoop上存储的大数据进行计算。

​    Flink（★★★）

​        Flink是当前最流行的开源大数据内存计算框架。用于实时计算的场景较多。



单机处理大数据存在的问题

