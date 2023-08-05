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
- 分布式计算框架Spark  速度十倍于MR
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

<img src=".\assets\image-20230723211553022.png" alt="image-20230723211553022"  />

![image-20230723211607724](.\assets\image-20230723211607724.png)

![image-20230723211615173](.\assets\image-20230723211615173.png)

![image-20230723211620603](.\assets\image-20230723211620603.png)



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

- 通过散列存储+计算，可以减少IO，但性能提升已经是极限（内存寻址比IO寻址快10万倍）
- 瓶颈在IO、内存太小（单机最大的内存SAP HANA数据库，内存2T 还要打包买服务器、软件 需要2亿）

数据分发+分布式存储+并行计算+网络传输，在数据不断增量的场景下有优势

![image-20230730145144673](assets/image-20230730145144673.png)

大数据技术的关键：分而治之、并行计算、计算向数据移动、数据本地化读取



05年

**Hadoop项目**包括，Hadoop common、Hadoop Distribution File System、Hadoop YARN、Hadoop MapReduce

**Hadoop生态圈**还包括：HBase、Hive、Spark、ZooKeeper...

Cloudera公司的CDH提供Hadoop生态圈的技术和版本线

- 接入层、存储层、统一服务层、处理解析服务（Impala是Cloudera自己的产品）
- 批式计算-》流式计算（spark flink还能实时，阿里主推，只需对增量部分计算） 

<img src="assets/image-20230730151249106.png" alt="image-20230730151249106" style="zoom:50%;" />

# HDFS

在此之前也有分布式文件系统，但设计HDFS的目的是**支持分布式计算**

## 存储模型

背死（数据块的特性）：大的文件划分块存在不同节点（分治），将计算向数据移动

逻辑模型：

- 文件线性按**字节**切割成块(block)，具有offset， id
  - 八个比特组成字节，计算机中的数据都是一堆字节数组。
  - 100字节数据=[10字节块，...，10字节块]，id从0到9 ，偏移量为10*id字节 
  - 按字节作为划分粒度，数据可能会出现割裂，但恢复是计算层管的
- 文件与文件的block大小可以不一样，一个文件除最后一个block，其他block大小一致
- block的大小依据硬件的I/O特性调整，不同应用框架默认值不同
  - 机械硬盘 500M/s
- **block被分散存放在集群的节点中，具有location**
  - 要知道块在哪，作为元数据存在NN
- Block具有副本(replication)，**没有主从概念**，副本不能出现在同一个节点
- 副本是满足**可靠性**和**性能**的关键
  - 不能出现在同一个节点，不是单机
  - 并行计算
- 文件上传可以指定block大小和副本数，**上传后只能修改副本数**
- 一次写入多次读取，**不支持修改**，但支持追加文件数据
  - 面向已写入的文件数据，便于多个客户端一致性访问
  - 不支持修改（泛洪操作）：数据修改多了，占用了网络带宽通信资源，影响应用计算层的使用
  - 最后一个块的存储节点，若还没满，可以加数据，不影响资源使用；即便没满，在新的集群生成块，也不影响资源使用，也被允许

## 架构设计

三个角色：客户端、NameNode DataNode；角色即（JVM）进程

- HDFS是一个主从(Master/Slaves)架构，由一个NameNode和一些DataNode组成
  - 主从：都是活动的，能协作互相通信，
  - 主备：用于高可用环境，主节点挂了才启用备用节点
- 面向文件包含：文件数据(data)和文件元数据(metadata)
  - 文件元数据相当于windows系统中的文件属性，用来描述文件的名称、大小、时间。。。
  - WIndows硬盘分区，再存目录文件。G:// 没法创建
  - Linux的分区会挂载到虚拟目录树，相当于对硬件分区做了解耦、映射。 ./g 能创建，会挂载到disk:G分区
- NameNode负责存储和管理文件元数据，并维护了一个层次型的**文件目录树**
  - 记录数据的描述信息（数据再DataNode中的路径、副本数量），并用于存储和管理文件元数据
- DataNode负责存储文件数据(block块)，并提供block的读写
  - 主节点提供了负载均衡的调度，与客户端短暂交流去哪读写，然后去找客户端DataNode
- DataNode与NameNode**维持心跳**，并汇报自己持有的block信息
  - 从节点告知主节点实际的存储数据情况，而不是根据客户端的反馈
- Client和NameNode**交互文件元数据**和DataNode**交互文件block数据**
  - 主节点的元数据要等从节点存储生效，再生效

<img src="assets/image-20230730170256577.png" alt="image-20230730170256577" style="zoom:50%;" />



## 角色功能

NameNode（NN）

- 完全**基于内存存储**文件元数据、目录结构、文件block的映射
  - 内存访问速度快，更快速对外客户端提供服务
  - 但内存数据易失，且大小有限。故需要**持久化方案**保证数据的可靠性
    - Redis、HBase、ElasticSearch都是有限把数据放到内存，选择的持久化方案都不太一样
- 需要**持久化方案**保证数据可靠性
- 提供**副本放置策略**

DataNode（DN）

- 基于本地磁盘存储block(**文件的形式**，物理形式)
- 还保存了block的**校验和数据**保证block的可靠性
- 与NameNode保持心跳，汇报block列表状态

SecondNameNode（SNN）

- 在非Ha模式（高可用模式，多个NN）下，SNN一般是独立的节点，专门用来周期完成对NN的EditLog向Fslmage合并，减少EditLog大小，减少NN启动时间
- 根据配置文件设置的时间间隔fs.checkpoint.period 默认3600秒（1h）
- 根据配置文件设置Editslog大小 fs.checkpoint.size 规定edits文件的最大值默认是64MB

<img src="assets/image-20230730195538696.png" alt="image-20230730195538696" style="zoom:50%;" />

- 通过SNN辅助持久化，而不再NN中滚动更新
- 企业不采用，还是需要多个NN，保证高可用

## 元数据持久化

数据持久化的两种方案：

- 日志文件：

  - 完整性好：日志文件存放所有增删改操作，支持append操作；完整性好，不会丢操作
  - 缺点：以文本文件存储，加载恢复数据慢，占内存空间

- 镜像、快照、dump、db、JAVA序列化

  - 间隔将内存全量数据在某个时间点写到磁盘，IO速度慢，容易丢数据
  - 以二进制文件存储，恢复速度快于日志文件

  补充：存储方式：文件编码: .txt，按UTF-8 编码，每个字符占用一个字节；二进制文件，int 类型占用4个字节，否则字符串类型，也是一个字符一个字节

HDFS这两种方式都使用了，日志：EditsLog，记录少时有优势；快照镜像：FsImage，更新频率高时有优势。采用最佳时点的FsImage+增量的EditsLog，恢复时先加载FI、再加载EL，具体持久化操作为：

- 任何对文件系统元数据产生修改的操作，Namenode都会使用一种称为EditLog的事务日志记录下来
- 使用Fslmage存储內存所有的元数据状态
- 使用本地磁盘保存EditLog和Fslmage
  - EditLog具有完整性，数据丢失少，但恢复速度慢，并有体积膨胀风险
  - Fslmage具有恢复速度快,体积与内存数据相当,但不能实时保存,数据丢失多
- NameNode使用了Fslmage+EditLog整合的方案；滚动将增量的EditLog更新到Fslmage，以保证更近时点的Fslmage和更小的EditLog体积
  - 增量的EL数据，可以用来更新FI，得到滚动后的FI

## 安全模式

搭建与进程启动

- HDFS**搭建时**会格式化，格式化操作会在硬盘产生一个空的Fslmage

- 当Namenode**启动时**，它从硬盘中读取Editlog和Fslmage，将所有Editlog中的事务作用在内存中的Fslmage上，并将这**个新版本的Fslmage从内存中保存到本地磁盘**上

- 然后删除旧的Editlog,因为这个旧的Editlog的事务都已经作用在Fslmage上了
- 持久化的元数据信息，有文件的存放属性信息，但没有节点的位置信息（不会持久化，恢复的时候，从节点可能挂了，数据可能丢失了，如果存之前的信息，会导致数据不一致），**需要通过安全模式的心跳建立额外检查获取**

安全模式，要保证存放数据块的节点是好的，能通信到的：

- Namenode启动后会进入一个称为安全模式的特殊状态。
- 处于安全模式的Namenode是不会进行数据块的复制的。
- Namenode从所有的 Datanode接收心跳信号和块状态报告。
- 每当Namenode检测确认某个数据块的副本数目达到这个最小值,那么该数据块就会被认为是**副本安全**(safely replicated)的。
- 在一定百分比（这个参数可配置）的数据块被Namenode检测确认是安全之后（加上一个额外的30秒等待时间），Namenode将退出安全模式状态。
- 接下来它会确定还有哪些数据块的副本没有达到指定数目，并将这些数据块复制到其他DataNode上。

## (block)副本放置策略

企业一般用的多的是机架服务器、多个机架服务器放在一个机柜、机柜机柜之间用交换机连接，机架内单独有个电源，称一个机柜为机架，机架内不同机器需要通过交换机进行一次IO通信，不同机架内的机器可能需要多次IO通信

块的副本放置策略

- 第一个副本：放置在上传文件的DN，即放在客户端本地；如果是集群外提交，则随机挑选一台磁盘不太满，CPU不太忙的节点。不需要通过交换机的网络IO通信
- 第二个副本：放置在于第一个副本不同的机架的节点上（**出机架**）。
- 第三个副本：与第二个副本相同机架的节点。2,3放一起就经过一个交换机的IO通信
- 更多副本：随机节点。

<img src="assets/image-20230730203816694.png" alt="image-20230730203816694" style="zoom:50%;" />

## 读写流程

最重要

写流程：

- Client和NN连接创建文件元数据
- NN判定元数据是否有效
  - 元数据是否有效，比如路径下已有该文件，是否有权限修改文件等
- **NN触发副本放置策略，返回一个有序的DN列表**
  - 根据距离策略排序得到的
  - Hadoop 中有多种距离策略可供选择，包括：
    - 默认距离策略：基于网络拓扑结构和延迟信息来计算 DataNode 与客户端之间的距离。这个距离策略是 Hadoop 中的默认策略。
    - 机架感知距离策略：在默认距离策略的基础上，进一步考虑了机架之间的距离和网络拓扑结构，使得在同一机架内的 DataNode 优先被选择。
    - 自定义距离策略：允许用户根据自己的需求自定义距离算法，例如考虑机器的 CPU 使用率、磁盘负载等因素，以更准确地计算 DataNode 与客户端之间的距离。
- Client和DN建立Pipeline连接
- Client将块切分成packet (64KB) ，并使用chunk (512B) +chucksum (4B)填充
- **流式传输**方式：
  - Client将packet放入发送队列dataqueue中，并向第一个DN）发送
  - 第一个DN收到packet后本地保存并发送给第二个DN
  - 第二个DN收到packet后本地保存并发送给第三个DN
  - 这一个过程中,上游节点同时发送下一个packet
    - 生活中类比工厂的流水线：结论：流式其实也是变种的**并行计算**
- Hdfs使用这种传输方式,副本数对于client是透明的
  - 中间有DN挂了，不影响，传输完成会通过心跳汇报给NN，如果少了，会继续传输给新的DN
- 当block传输完成，DN们各自向NN汇报，同时client继续传输下一个block，所以，client的传输和block的汇报也是并行的

<img src="assets/image-20230730222124018.png" alt="image-20230730222124018" style="zoom: 67%;" />

读流程：

- 为了降低整体的带宽消耗和读取延时，HDFS会尽量让读取程序**读取离它最近的副本**（同一机架，或离自己机架最近的，与写流程建立连接前一样）。
- 如果在读取程序的同一个机架上有一个副本，那么就读取该副本。
- 如果一个HDFS集群跨越多个数据中心，那么客户端也将首先读本地数据中心的副本。
- 语义：下载一个文件：
  - Client和NN交互文件元数据获取fileBlockLocation
  - NN会按距离策略排序返回
  - Client尝试下载block并校验数据完整性
- 语义（**最重要**:）下载一个文件其实是获取文件的所有的block元数据，那么子集获取某些block应该成立
  - 通过偏移量，支持只读取几个块，从而找到对应数据，快速建立并行计算
  - **Hdfs支持client给出文件的offset，自定义连接哪些block的DN，自定义获取数据**
  - **这个是支持计算层的分治、并行计算的核心**



<img src="assets/image-20230730222240807.png" alt="image-20230730222240807" style="zoom:67%;" />

安全策略



## 实验

### 环境配置

GNU/Linux+JAVA1.8+ssh

- JAVA移动性好，编译一次，去哪都能执行；需要给每台机器设置$JAVA_HOME环境变量
- Hadoop Script脚本 + ssh**免密远程执行** ，可以自动化启动集群 `ssh 用户名@ip '脚本命令'`
  - 在该目录下 修改环境变量后，需要`source /ect/profile `或使用其别名 `. /etc/profile`，这样可以在不重新启动终端的情况下，立即加载配置文件中的更改。这样，添加的环境变量将立即在当前的 Shell 环境中生效，而不需要重新启动终端。
  - 远程执行， `ssh 用户名@ip 'echo 环境变量'`失败，无法访问远程机器的环境变量（/etc/profile），这是因为没有加载/etc/profile文件；需要先 `ssh 用户名@ip 'source /ect/profile;echo 环境变量'`

```bash
#设置网络 IP

# 设置主机名
vi /etc/sysconfig/network
	NETWORKIN=yes
	HOSTNAME=node1
	
# 设置本机的IP到主机名的映射  解耦 因为IP可能改变，逻辑名称-》ip地址
vi /etc/hosts
	本机IP地址 别名
	
# 关闭防火墙 并禁止服务在系统启动时自动启动（systemctl或chkconfig）
service iptables stop
systemctl iptabels off
# 关闭selinux
vi /etc/selinux/config
	SELINUX=disable
	
# 做时间同步  使用阿里云的时间同步服务器 os会去和他同步
apt-get install ntp -y
vi /etc/ntp.conf
	server ntp1.aliyun.com
service ntpd start
systemctl ntpd on 

# 安装JDK  centos可以直接使用rpm包安装而不是压缩包（否则要配置很多命令） Ubuntu需要alien转换rpm包 再通过dpkg安装
rpm -i jdk***.rpm # 可能会生成/usr/local/java目录的软链接 别名指向该目录，如default 表示/usr/local/java/default 等价于/usr/local/java

# 配置JAVA_HOME  path为取出之前的path 再用: 拼起来JAVA的bin目录
vi /etc/profile
	export JAVA_HOME=/usr/local/java/jdk1.8.0_351
    export PATH=$PATH:$JAVA_HOME/bin
source /etc/profile # . /etc/profile
echo $JAVA_HOME # 检查

# ssh 免密设置 （考虑单机还是多机）
# Hadoop官网有  生成秘钥和公钥用于加解密
ssh localhost  # 验证自己还没有免密，被动会生成 /root/.ssh目录  ll -a 可查看到 进入该目录 有known_hosts文件

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa # 加密算法 空密码 放的路径，会生成两文件 秘钥和公钥
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys # 把自己的公钥id_rsa.pub文本内容 追加写入	自己或其他机器的authorized_keys文件中
chmod 0600 ~/.ssh/authorized_keys 

# 解压hadoop压缩包
tar xf hadoop*.tar.gz
# 移动到/opt/*目录

# 添加环境变量（操作系统的环境变量） 在容易位置可以执行hadoop命令
vi /etc/profile
	export HADOOP_HOME=...
	export PATH=...:HADOOP_HOME/bin:HADOOP_HOME/sbin

source /etc/profile

# 进入hadoop/etc目录 给Hadoop配置JAVA环境和HDFS角色 官网有，
cd $HADOOP_HOME/etc/hadoop
vi hadoop_env.sh # 默认配置为取环境变量${JAVA_HOME} 这在另一台机器上访问取不到该环境变量 修改为绝对路径
	export JAVA_HOME=/usr/local/java/jdk1.8.0_351

vi core_site.xml # 设置NameNode 告诉客户端和DN节点 NN在哪 不写localhost，写本机的主机名node01
    <configuration>
        <property>
            <name>fs.defaultFS</name>
            <value>hdfs://node01:9000</value>
        </property>
    </configuration>
vi hdfs_site.xml 
    <configuration>
        <property> # 放置数据块的副本数 伪分布式的value=1 因为副本不能放在同一节点 
            <name>dfs.replication</name>
            <value>1</value>
        </property>
        <property> # 补充元数据和数据块的放置目录 从而不被OS作为临时文件以删除
            <name>dfs.namenode.name.dir</name>
            <value>/var/bigdata/hadoop/local/dfs/name</value>
        </property>
        <property> # 补充元数据和数据块的放置目录 从而不被OS作为临时文件以删除
            <name>dfs.datanode.data.dir</name>
            <value>/var/bigdata/hadoop/local/dfs/data</value>
        </property>
        <property> # 补充SecondNameNoded与NN的通信地址
            <name>dfs.namenode.secondary.http-address</name>
            <value>node01:9868</value>
        </property>
        <property> # 补充SecondNameNoded用于与NN合并元数据的目录
            <name>dfs.namenode.checkpoint.dir</name>
            <value>/var/bigdata/hadoop/local/dfs/namesecondary</value>
        </property>

    </configuration>
	 #
vi slaves # 设置DataNode DN在哪
	node01
	

	
```

> 免密登陆：
>
> - 机器A的.ssh/authorized_keys文件有了机器B的公钥 A就能免密ssh登录B（满足自己免密登自己）
> - 公钥是由私钥生成的，但是无法从公钥中推导出私钥。**公钥用于加密**从客户端发送到服务器的数据。而**私钥则用于解密这些数据**。这意味着只有持有正确的私钥的用户才能解密通过公钥加密的数据。
> - 身份验证：在免密登录过程中，你需要将你的公钥提供给需要免密登录的机器，以便在身份验证过程中使用。当你尝试连接到远程机器时，**远程机器会比对你提供的公钥和已经存储在其上的公钥。如果两者匹配，那么你就可以成功进行免密登录。**
>
> 权限设置
>
> - 3个数字分别表示用户 组 其他人的权限 
> - 每个数字取0-7，代表：没有权限（无法读取、写入或执行）；执行权限；写入权限；写入和执行权限；读取权限；读取和执行权限；读取和写入权限；读取、写入和执行权限
>
> hadoop目录文件
>
> - hadoop/sbin目录下存放服务启停的脚本，hadoop/bin目录下存放应用功能命令模块；hadoop/etc存放配置；hadoop/share存放jar包
>
> 为什么要在hadoop_env.sh中需要再设置JAVA_HOME
>
> - SSH免密登录过程中，不会执行/etc/profile文件，获取系统的JAVA环境变量，而只接受hadoop_env.sh里的环境变量。
>
> 额外的HDFS配置，查看[官方xml配置文档](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml)
>
> - core-default.xml中默认hadoop.tmp.dir=/tmp/hadoop-${user.name}，在本地存放临时数据的目录
> - NN将元数据持久化为FI和EL存在本地，DN将数据块存在本地，hdfs-default.xml中默认了NN的存放目录为dfs.namenode.name.dir=file://\${hadoop.tmp.dir}/dfs/name；和DN的存放目录为dfs.datanode.data.dir=file://\${hadoop.tmp.dir}/dfs/data，**即将这些数据作为临时数据存放**
> - 然而/tmp目录是一个OS可以在磁盘不够下删除的目录，因此存在风险，为了数据的安全可靠，需要再HDFS中修改存放临时数据的目录
> - 修改时注意命名规则，dfs.namenode.name.dir=某一指定目录/dfs/name
> - SecondaryNameNode角色的默认通信地址为dfs.namenode.secondary.http-address=0.0.0.0:9868，由主机地址:端口号组成。
>   - SecondaryNameNode 需要与 本地的NameNode 建立通信，因此需要指定主机地址和端口号来与 NameNode 进行通信。
> - dfs.namenode.checkpoint.dir=file://${hadoop.tmp.dir}/dfs/namesecondary，SNN存放临时镜像的目录



### HDFS启动

1. Format the filesystem：执行/bin目录下的hdfs程序，`hdfs namenode -format`，成功执行下只需要格式化一次

   - **创建NN的数据存放目录name**

   - 初始化一个空的FI

   - 生成VERSION文件，存放集群ID（CID）
     - 一个集群有一个NN和多个DN，一个集群的DN无法和另一个集群的NN通信

![image-20230805174321400](assets/image-20230805174321400.png)

2. Start NameNode daemon and DataNode daemon：执行/sbin下的start-dfs.sh程序`start-dfs.sh`，他会调用slave.sh文件，使用 ssh 连接到远程主机 `$slave`
   - 使用 SSH 并执行 `$@"` 中指定的命令。`$@"` 表示将脚本接收到的所有参数传递给该命令。
   - `$HADOOP_SSH_OPTS` 是用于 SSH 连接的选项和参数。
   - `2>&1` 将标准错误输出重定向到标准输出，以便将错误消息传递给 `sed` 命令。
   - `sed "s/^/$slave: /"` 在输出中添加前缀 `$slave:`，以标识输出来自哪个主机。
   - `&` 将命令放入后台执行，以便可以同时启动多个从节点。

![image-20230805185253569](assets/image-20230805185253569.png)

3. `start-dfs.sh`会读取HDFS配置文件（hdfs_site.xml，slaves.sh），启动NN，DN，SNN进程

   - 第一次启动会创建SNN的数据存放目录secondary和DN的数据目录data
   - 生成的data目录下存放DN的VERSION文件，与NN节点数据目录下的VERSION文件，有相同的集群ID，CID

   ![image-20230805195524014](assets/image-20230805195524014.png)

4. `jps`查看到有三个角色进程

5. windows的 C:\Windows\System32\drivers\etc\hosts文件中可添加IP地址和别名，从而在浏览器中访问 http://node01:50070 （50070端口）

   > IP node01

### HDFS使用

