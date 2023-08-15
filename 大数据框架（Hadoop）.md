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
  - 要知道块在哪，作为元数据存在于NN
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
  - 内存访问速度快，更快速对外客户端提供服务（内存中元数据通常以树或哈希表的形式组织）
  - 但内存数据易失，且大小有限。故需要**持久化方案**保证数据的可靠性
    - Redis、HBase、ElasticSearch都是有限把数据放到内存，选择的持久化方案都不太一样
- 需要**持久化方案**保证数据可靠性
- 提供**副本放置策略**

DataNode（DN）

- 基于**本地磁盘存储**block(**文件的形式**，物理形式)
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

读写都需要访问NN上的元数据

写流程：

- Client和NN连接，创建文件元数据
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

### 基础环境配置

GNU/Linux+JAVA1.8+ssh

- JAVA移动性好，编译一次，去哪都能执行；需要给每台机器设置$JAVA_HOME环境变量
- Hadoop Script脚本 + ssh**免密远程执行** ，可以自动化启动集群 `ssh 用户名@ip '脚本命令'`
  - 在该目录下 修改环境变量后，需要`source /ect/profile `或使用其别名 `. /etc/profile`，这样可以在不重新启动终端的情况下，立即加载配置文件中的更改。这样，添加的环境变量将立即在当前的 Shell 环境中生效，而不需要重新启动终端。
  - 远程执行， `ssh 用户名@ip 'echo 环境变量'`失败，无法访问远程机器的环境变量（/etc/profile），这是因为没有加载/etc/profile文件；需要先 `ssh 用户名@ip 'source /ect/profile;echo 环境变量'`
- linux的第三方软件一般放/opt目录，数据存放在/var目录下

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
>   - 数字0-7的产生：由rwx表示”读 写 打开目录“，取1表示可以，0表示不行，组成3位二进制
>
>
> hadoop目录文件
>
> - hadoop/sbin目录下存放HDFS服务和其他服务启停的脚本(start-dfs.sh、start-yarn.sh)，hadoop/bin目录下存放应用功能命令模块(hadoop hdfs yarn)；hadoop/etc存放配置；hadoop/share存放jar包
>
> <table>
>   <tr>
>     <td>
>       <img src="assets/image-20230806093712768.png" alt="Image 1">
>     </td>
>     <td>
>       <img src="assets/image-20230806094353382.png" alt="Image 2">
>     </td>
>   </tr>
> </table>
>
> 为什么要在hadoop_env.sh中需要再设置JAVA_HOME
>
> - SSH免密登录过程中，不会执行/etc/profile文件，获取系统的JAVA环境变量，而只接受hadoop_env.sh里的环境变量。
>



### 伪分布式配置

Hadoop的部署配置：Hadoop集群的三种模式，不同模式的配置不同

- [Local (Standalone) Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation)：一个JAVA进程，担当三个角色
- [Pseudo-Distributed Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)：一个角色启动一个JAVA进程，但在一个节点启动所有角色
- [Fully-Distributed Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Fully-Distributed_Operation)：

以伪分布式的配置为例，在$HADOOP_HOME/etc/hadoop目录下修改以下配置文件

```bash
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
        <property> # 补充元数据和数据块放置目录（为了持久化） 从而不被OS作为临时文件以删除
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

vi slaves # 设置DataNode DN在哪
	node01
```

>额外的HDFS配置，查看[官方xml配置文档](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml)
>
>- core-default.xml中默认hadoop.tmp.dir=/tmp/hadoop-${user.name}，在本地存放临时数据的目录
>- NN将元数据持久化为FI和EL存在本地，DN将数据块存在本地，hdfs-default.xml中默认了NN的存放目录为dfs.namenode.name.dir=file://\${hadoop.tmp.dir}/dfs/name；和DN的存放目录为dfs.datanode.data.dir=file://\${hadoop.tmp.dir}/dfs/data，**即将这些数据作为临时数据存放**
>- 然而/tmp目录是一个OS可以在磁盘不够下删除的目录，因此存在风险，为了数据的安全可靠，需要再HDFS中修改存放临时数据的目录
>- 修改时注意命名规则，dfs.namenode.name.dir=某一指定目录/dfs/name
>- SecondaryNameNode角色的默认通信地址为dfs.namenode.secondary.http-address=0.0.0.0:9868，由主机地址:端口号组成。
>  - SecondaryNameNode 需要与 本地的NameNode 建立通信，因此需要指定主机地址和端口号来与 NameNode 进行通信。
>- dfs.namenode.checkpoint.dir=file://${hadoop.tmp.dir}/dfs/namesecondary，SNN存放临时镜像的目录



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

4. `jps`查看到有三个角色的JAVA进程

5. windows的 C:\Windows\System32\drivers\etc\hosts文件中可添加IP地址和别名，从而在浏览器中访问 http://node01:50070 （50070端口）

   > IP node01

### HDFS使用

hdfs dfs + <-命令>，在HDFS中**创建用户数据文件**，在而不是单纯本地机器的文件系统中，和各角色的数据存放目录存在本地不同（取决于是不是完全的分布式，将数据存放目录配置在其他机器上）

1. 在NN数据目录中创建目录：`hdfs dfs -mkdir 目录`，如`hdfs dfs -mkdir -p usr/root`，创建home目录（多级目录 -p）

2. 上传文件：`hdfs dfs -put 文件 目录`，可以看到文件以块大小被划分到DN（默认放到home目录下）

   <img src="assets/image-20230806101458721.png" alt="image-20230806101458721" style="zoom:50%;" />

<img src="assets/image-20230806101620534.png" alt="image-20230806101620534" style="zoom:50%;" />

3. 在本地的DN数据存放目录下可查看到对应的数据块和数据库检验信息（.meta）

   ![image-20230806101942120](assets/image-20230806101942120.png)

4. 验证块的线性切割

   1. 输入bash脚本命令，生成一个文件

      ```bash
      for i in `seq 100000`;do echo "hello hadoop $i" >> data.txt ;done'
      ```

   2. `ll -l -h`可查看到该文件的大小为1.9M（**-h**uman 表示人可读的文件大小）

   3. `hdfs dfs -D dfs.blocksize=1048576 -put data.txt`，-D指定属性=值，属性来自配置信息，将data.txt以1M=1024*1024=1048576bit的大小进行数据块分割

   4. 去data目录查看两个数据块文件的结尾和开头，发现就是直接切割的

      <table style="display: flex; justify-content: center;">
        <tr>
          <td>
            <img src="assets/image-20230806111142016.png" alt="Image 1">
          </td>
          <td>
            <img src="assets/image-20230806111127953.png" alt="Image 2">
          </td>
        </tr>
      </table>



### 完全分布式配置

在伪分布式的基础上，在配置文件中规划角色放在哪些节点上，这里将按照以下规划

![image-20230806144335206](assets/image-20230806144335206.png)

1. 配置相同的基础环境（JAVA HADOOP，可以通过scp直接-r 分发/opt/bigdata/..），稍有不同的是

   - 保证各个几点的/etc/hosts文件存放其他节点，保证能互相通信

   - 设置SSH免密（HA高可用环境下，公钥的分发与角色相关，现在默认谁执行 start-dfs.sh，谁把公钥分发给其他节点）
     - node01向node02分发公钥，`scp ./ssh/id_rsa.pub node02:/home/.ssh/node01.pub`
       - node02追加公钥到authorized_keys，`cat node01.pub >> authorized_keys  `
       - 其他机器同理，就可以完成node01免密登录其他机器
       - 使用scp时，若路径相同可以简写为 scp id_rsa.pub node02:\`pwd\`/node01.pub

2. 在 $HADOOP_HOME/etc/hadoop中配置 在哪启动角色，角色数据的保存地址

   - 可以将伪分布式的配置做个拷贝，`cp hadoop hadoop-local`，运行start-dfs.sh执行程序，将默认进入/etc/hadoop目录下读取配置，可以只是做个备份

   - 不修改本机的core-site.xml，规定NN在本机上启动

   - 修改hdfs_site.xml，配置DN和SNN

     -  SecondNameNoded放在node02节点

     ```xml
     <configuration>
         <property> 
             <name>dfs.replication</name>
             <value>2</value>
         </property>
         <property> 
             <name>dfs.namenode.name.dir</name>
             <value>/var/bigdata/hadoop/full/dfs/name</value>
         </property>
         <property> 
             <name>dfs.datanode.data.dir</name>
             <value>/var/bigdata/hadoop/full/dfs/data</value>
         </property>
         <property>
             <name>dfs.namenode.secondary.http-address</name>
             <value>node02:9868</value>
         </property>
         <property> 
             <name>dfs.namenode.checkpoint.dir</name>
             <value>/var/bigdata/hadoop/full/dfs/namesecondary</value>
         </property>
     </configuration>
     ```

3. 修改slaves，修改DN在哪启动
   - 按 shfit zz等价于wq

```bash
vi slaves
	node02
	node23
	node04
```

 4. 分发？，为什么要分发本地之前创建的 bigdata目录

 5. 格式化启动

    `hdfs namenode -format`

    `start-dfs.sh`

    将会在各个节点创建角色和数据保存目录

## HA高可用

只有一个NN，存在两个独立的问题

- 单点故障，集群不可用
- 压力过大，内存受限（都要和同一台NN节点通信，元数据存在内存，用于快速访问）

需要采用两种独立的解决方案，分别为

- 高可用方案（HA，High Available），多个NN，主备切换
  - 其中HADOOP2.X 上线较为仓促，只支持HA的一主一备；HADOOP3.X支持一主多备（最多五个）
- 联邦机制（Federation），将元数据分片；多个NN管理不同的元数据

### HA方案

备用NN不对外提供服务

NN的元数据来自客户端的增删改写入操作和DN提交的数据块信息，**后者会在写入NN时，同步写入到备用NN；而来自客户端的写入，只与NN通信（这是为了简化客户端的操作和管理，快速响应，减少通信浪费）**，因此需要保证备用NN一致性，否则NN挂了，还没把客户端来的操作写给备用NN

- 分布式场景下的一致性会破坏可用性（通信模型的同步阻塞），要等延迟时间，备用节点写好

- 分布式场景下的可用性会破坏一致性（通信模型的异步非阻塞），不知道给备用节点写好了没有

- 即存在CAP原则，全满足不可能

  <img src="assets/image-20230806204948400.png" alt="image-20230806204948400" style="zoom:50%;" />

JN（Journal Node）：为了在分布式场景下尽可能的保证一致性和可用性，需要在NN和备用NN间构建中间件（而且是多个，三个以上，保证过半同步），其保证**可靠存储**，且是异步执行（实现分布式存储，一定能给主备NN快速返回正确的数据，实现数据同步）

- 主的选举：明确节点数量和权重，选出权重最高的作为主；主挂了，会自动从无主状态选出主
  - 权重分配方式，有两种：
    - 1、看手工权重；
    - 2、看数字ID：每次写入会有一半有最新的ID，数据ID相同看随机ID：取主机里头的某个信息（IP）生成一个值，保证不重复和冲突
- 主的职能：增删改查，NN与主直接通信，主将信息同步给从
- 从的职能：查询，增删改传递给主。一半的从返回写入成功则返回给主
- 主与从：**过半数同步数据**，从而中和一致性和可用性

- 保证可靠：中间件是一个**主从集群**，保证一定有主与NN进行过半同步（内部一致）；备用NN只要能连接上就可以与JN的一致性写入进行同步
- 保证可用：有一半从节点返回写入成功

NN的主备切换：以上使用JN能实现HA（可用和一致），当主NN挂掉，需要手动将备用NN升级为active的主NN，即还需要手工确定谁是主NN，谁是备用NN，该切换方式效率明显很低，需要自动化的选主NN

- ZooKeeper FailoverController进程（ZKFC，故障转移），与NN进程在同一个物理机上（不需要网络监控，非常可靠），并与分布式协调服务ZooKeeper集群连接
  - 初始化：在HDFS HA架构启动时，FailoverController进程会连接到ZooKeeper集群，以建立与ZooKeeper的通信通道。
  - 注册：FailoverController会在ZooKeeper上创建一个**临时的、有序的节点**，表示当前FailoverController的存在。这样可以在ZooKeeper上形成一个FailoverController的有序列表。**临时节点申请的锁是临时锁，一旦临时节点挂掉，会直接触发临时锁的删除事件**
  - 心跳：FailoverController定期向ZooKeeper发送心跳信号，以表明它的存活状态。这样ZooKeeper可以检测到FailoverController是否正常运行。
  - 监视：其他FailoverController进程也会在ZooKeeper上创建相同类型的临时节点。这些进程会监视ZooKeeper上的节点列表，以侦听主节点状态的变化。
- ZKFC进程执行三件任务
  - 健康检查：实时监控主/备NN是否active
  - 抢锁：连接ZooKeeper集群，ZKFC要向ZooKeeper创建一个锁，表示创建一个ZooKeeper的子目录，只有一个ZKFC能创建成功，谁创建成功，谁主机下的NN是主，其他是备用NN；并且申请到的锁，会注册一个回调函数
  - 主备切换：ZKFC得知本机的NN挂了，将抢来的锁删除（也可能锁到期），触发ZooKeeper中锁的删除事件，调用申请时的回调函数，其他的ZKFC会开始新一轮的抢锁（使用回调函数，保证实时响应主备切换，而不是间隔抢锁，存在等待），抢到锁的ZKFC，会查看原来机器上的NN是不是真的挂掉了，对方真的挂了，才把本地的NN升为Active（或对方没挂，直接锁到期，将对方的NN降为Standby）
    - 选举：如果多个FailoverController同时检测到主节点失效，它们会基于ZooKeeper上节点的有序列表来进行选举，以决定新的主节点。
- 除了自己本机进程通信，其他通信都需要网络连接，因此存在不可靠性
  - 当NN Active和ZKFC的机器无法与外界通信，只能等到锁的到期，但抢到锁的ZKFC也无法与那台机器连接到，查看其是否挂掉，因此出现问题（bug，但很少发生）
  - 使用串口线将每台NN主机进行连接，能连到电源线，能直接将发生故障的Active NN的主机断电，再放心的将备用的NN升级为Active

<img src="assets/image-20230806163422616.png" alt="image-20230806163422616" style="zoom:67%;" />

HA方案总结：

- 多台NN主备模式，Active和Standby状态
  - Active对外提供服务
- 增加journalnode角色（>3台），负责同步NN的EditLog，实现最终一致性
- 增加ZKFC角色(与NN同台)，通过ZooKeeper集群协调NN的主从选举和切换
  - 事件回调机制，提升切换效率
- DN同时向所有NN汇报block清单，能保证一致性
- HA模式下没有SNN，备用NN不对外提供服务，能代替SNN周期性地利用同步过来的EL滚动生成FsImage，实时发给NN，加快NN的下一次启动

### 联邦机制

企业用的没那么多，元数据没那么大

解决NN的压力过大，内存受限问题；名字取自美国州的联邦制

<img src="assets/image-20230812215829663.png" alt="image-20230812215829663" style="zoom:50%;" />

- 元数据分治：将元数据存在不同的NN中，并复用同一个DN存储，存储在不同的**DN目录**中隔离不同block

  - 解决元数据过大，内存压力大的问题

- 元数据访问隔离性：客户端连接一个NN，只能访问对应元数据规定的数据块

  - 会产生一定的用户访问体验度下降（就像看到所有的数据）

  - 可通过一个虚拟文件系统，同时拥有不同的NN元数据，能访问所有数据；同理可升级成一个文件存储平台，可代理不同的文件存储（HDFS、FTP），只需提供相应的接口即可

    <img src="assets/image-20230812220848405.png" alt="image-20230812220848405" style="zoom:50%;" />

## 配置HA实验

![image-20230812223510573](assets/image-20230812223510573.png)

【注】：HDFS HA有两种技术实现，一个是上面说的JN实现，一种是Linux自带的NFS技术（Network for System），后面这种将两台机器的目录都挂载到远程单点机器的目录中，实现一致性和可用；配置HA前，保证没有HDFS进程在运行，否则需要在执行start-hdfs.sh的机器上执行stop-dfs.sh停止服务

1. 基础设施配置，主要需要进一步更改SSH免密
   - 保证start-hdfs.sh的机器，把公钥发给其他机器，能够免密启动DN
   - 额外需要将ZKFC进程能够免密查看和控制本机的NN和其他机器上的NN，即**有NN的机器需要互发公钥**
2. 应用配置
   - HA需要额外ZooKeeper集群
   - 修改HADOOP配置，与集群同步
3. 初始化启动（1-5是搭建时做的，后续只需要做6启动 start 停止stop）
   1. 先启动JN  `hadoop-daemon.sh start journalnode`
   2. 选择一个NN做主，进行格式化 `hdfs namenode -format`
   3. 启动这个NN，以备其他NN同步元数据 `hadoop-daemon.sh start namenode`
   4. 在另外一台机器上，同步与主NN的元数据 `hdfs namenode -bootstrapStandby`
   5. 格式化ZooKeeper自动切换主备`hdfs zkfc -formatZK`
   6. 启动集群 `start-dfs.sh` 

### 应用配置

```bash
# 在node02中配置信息
tar xf zookeeper*.tar.gz /opt/bigdata

# 添加环境变量
vi /etc/profile
	export ZOOKEEPER_HOME=/opt/bigdata/zookeeper*
	export PATH=$PATH:ZOOKEEPER_HOME/bin # 加上
. /etc/profile

# 配置ZooKeeper集群 使他们在各台机器上启动时 能形成集群
cd /opt/bigdata/zookeeper*/conf
cp zoo_sample.cfg zk.cfg
vi zk.cfg
	 dataDir=/tmp/zookeeper  修改为 dataDir=/var/bigdata/hadoop/zk
	 server.1=node02:2888:3888
	 server.2=node03:2888:3888
	 server.3=node04:2888:3888
cd /var/bigdata/hadoop
mkdir /zk
echo 1 > /zk/myid # 写入权重

# 分发到node03 node04中
cd /var/bigdata/
scp -r ./zookeeper node03:`pwd`
scp -r ./zookeeper node04:`pwd`

# 同样加入ZOOKEEPER_HOME 并在其他机器上不上权重文件
mkdir /var/bigdata/hadoop/zk
echo 2 > /var/bigdata/hadoop/zk/myid

mkdir /var/bigdata/hadoop/zk
echo 3 > /var/bigdata/hadoop/zk/myid

# 启动zookeeper 将启动ZK服务器，将配置
zkServer.sh start

# 修改HADOOP配置
cd $HADOOP_HOME
cd etc 
## core_site.xml配置
cd hadoop
vi core_site.xml 
    <configuration>
        <property>
            <name>fs.defaultFS</name> # 将物理主机名改为hdfs中指定的集群逻辑地址mycluster（会自动解析成主机）
            <value>hdfs://mycluster</value>
        </property>
        
        <property> # 指定ZKFC访问的ZooKeeper集群地址  ,隔开
            <name>ha.zookeeper.quorum</name>
            <value>node02:2181,node03:2181,node04:2181</value>
        </property>
    </configuration>
## hdfs_site.xml配置
vi hdfs_site.xml
    <configuration>
        <property> 
            <name>dfs.replication</name>
            <value>2</value>
        </property>
        <property> 
            <name>dfs.namenode.name.dir</name>
            <value>/var/bigdata/hadoop/ha/dfs/name</value>
        </property>
        <property> 
            <name>dfs.datanode.data.dir</name>
            <value>/var/bigdata/hadoop/ha/dfs/data</value>
        </property>
        
        # 配置逻辑地址mycluster到不同NN物理地址一对多的映射   
        <property>
          <name>dfs.nameservices</name>
          <value>mycluster</value>
		</property>
        <property>
          <name>dfs.ha.namenodes.mycluster</name>
          <value>nn1,nn2</value>
        </property>

        <property>
          <name>dfs.namenode.rpc-address.mycluster.nn1</name>
          <value>node01:8020</value>
        </property>
        <property>
          <name>dfs.namenode.rpc-address.mycluster.nn2</name>
          <value>node02:8020</value>
        </property>

        <property>
          <name>dfs.namenode.http-address.mycluster.nn1</name>
          <value>node01:9870</value>
        </property>
        <property>
          <name>dfs.namenode.http-address.mycluster.nn2</name>
          <value>node02:9870</value>
        </property>

        # 配置启动JN的物理地址(不同机器共享同一个JN的不同目录地址)和数据存储目录 
        <property>
          <name>dfs.namenode.shared.edits.dir</name>
          <value>qjournal://node01:8485;node02:8485;node03:8485/mycluster</value>
        </property>
        
        <property>
          <name>dfs.journalnode.edits.dir</name>
          <value>/var/bigdata/hadoop/ha/dfs/jn</value>
        </property>

        # 配置HA角色切换的代理类和切换实现的方法，使用的是SSH免密（还有写脚本的方法）
        <property>
          <name>dfs.client.failover.proxy.provider.mycluster</name   
                  <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
        </property>
		
        <property>
          <name>dfs.ha.fencing.methods</name>
          <value>sshfence</value>
        </property>
        <property>
          <name>dfs.ha.fencing.ssh.private-key-files</name>
          <value>/root/.ssh/id_rsa</value>
        </property>

        # 配置HA自动化 启动hdfs时自动启动zkfc（与NN同机）
         <property>
           <name>dfs.ha.automatic-failover.enabled</name>
           <value>true</value>
         </property>

    </configuration>

# 分发配置到其他机器上
scp core-site.xml hdfs-site.xml node02:`pwd`
scp core-site.xml hdfs-site.xml node03:`pwd`
scp core-site.xml hdfs-site.xml node04:`pwd`
```

- zookeeper的/bin目录下有 zkServer.sh,zkCli.sh；/conf目录下有zoo_sample.cfg，文件中写了在内存中存数据的临时目录，需要将其修改为可靠目录，并添加服务器节点的配置信息

  - `server.<ID>=<hostname>:<peerPort>:<leaderPort>`

  - 指定了ZooKeeper集群中的每个不同服务器节点的信息，包括它们的地址和用于通信和选举的两个端口

    给定至少三个ZK节点，存在至少两台时能够相互通信，则可以选举出主NN（Leader节点），其他作为备用NN

  - ZooKeeper的选举算法会考虑节点的可用性和通信延迟等因素，以选择合适的Leader节点。通常情况下，ZooKeeper会选择具有最高可用性和最低延迟的节点作为Leader节点。

    - 在每天zookeeper节点机器的数据存放目录/var/bigdata/hadoop/zk中创建文件 myid，写入该节点的权重值


zookeeper leader选举

- 首先在node02启动zookeeper，查看节点状态zkServer.sh status和java进程，此时整个Zookeeper集群只有一个zk进程，但zk配置文件存着三个节点，当前节点无法与其他节点连接到，因此停止了服务“Error contacting service”

  ![image-20230813170102511](assets/image-20230813170102511.png)

- 在node03中启动zk，node03的节点状态为leader，node02上的节点状态处于follower（因为node03上的节点权重较大，三台节点，有两台能通信，可以选举出leader，建立可用状态）

  ![image-20230813170347978](assets/image-20230813170347978.png)

  ![image-20230813170324462](assets/image-20230813170324462.png)

- 在node04中启动zk，node04节点状态和node02一样为follower，node03为leader（即便node04的节点权重更大，但只要集群有leader处于可用状态，新启动的节点都直接作为follower，没必要再选举切换主从，导致不稳定）

  ![image-20230813170521945](assets/image-20230813170521945.png)



### 初始化启动

1-5是搭建时做的，后续只需要做6启动start/停止stop HDFS HA，但如果有角色挂了，需要`hadoop-daemon.sh start <角色>` 去启动该角色

⭐️需要学会查看JN，ZKFC的日志，在ZK客户端查看锁的变化⭐️

1. 先在node01 02 03中启动JN  `hadoop-daemon.sh start journalnode`，将创建jn的数据存放目录/var/bigdata/hadoop/ha/dfs/jn

   - 在node01中进入 /jn目录，是个空目录，因为NN还没有数据与JN同步

     ![image-20230814213126677](assets/image-20230814213126677.png)

   - 可以在node03机器的$HADDOOP_HOME/logs目录下，查看启动Jnode03机器上JN的最后几条日志，可以看到进程已经启动起来，但和node01的JN数据目录一样，现在还是空目录

     ![image-20230814211722573](assets/image-20230814211722573.png)

2. 在node01，进行格式化 `hdfs namenode -format`

   - 格式化成功，生成name目录，并存放着FI和VERSION文件

   ![image-20230814211510864](assets/image-20230814211510864.png)

   - VERSION文件中可查看到集群ID

   - 查看node03机器的JN log日志，发现这台机器在初始化并创建 jn/mycluster目录（其他机器上JN数据目录同理）

     <img src="assets/image-20230814212034786.png" alt="image-20230814212034786" style="zoom:67%;" />

   - node03对应目录下的的集群id与node01的一样

     <img src="assets/image-20230814212228917.png" alt="image-20230814212228917" style="zoom: 67%;" />

3. 在node01启动主NN，以备其他NN同步元数据 `hadoop-daemon.sh start namenode`

   <img src="assets/image-20230814212946406.png" alt="image-20230814212946406" style="zoom:67%;" />

4. 在node02启动备用NN，以同步与主NN的元数据 `hdfs namenode -bootstrapStandby`

   - 这台机器也会生成 name目录，存放元数据，也有这相同的集群ID

     ![image-20230814212545887](assets/image-20230814212545887.png)

     <img src="assets/image-20230814212629407.png" alt="image-20230814212629407" style="zoom:67%;" />

5. 格式化ZooKeeper以**自动切换**主备 `hdfs zkfc -formatZK` （启动ZK已在配置中完成，相当于连接到了ZK服务器，此时使用zkfc去格式化ZK集群）

   ![image-20230815205217903](assets/image-20230815205217903.png)

   在node04中打开ZK的客户端 `zkCli.sh`，`ls /`显示ZK根目录，可以看到格式化ZK集群后，生成了空目录

   ![image-20230815205337749](assets/image-20230815205337749.png)

   

6. 在node01启动集群 `start-dfs.sh` 

   - 可以看到角色：成功在node01 02启动NN ZKFC，在node02 03 04启动DN，在node01 02 03启动JN![image-20230815205855833](assets/image-20230815205855833.png)

   - 在node03的日志中，将发生JN与NN的数据同步

     ![image-20230815205950154](assets/image-20230815205950154.png)

     ![image-20230815210036801](assets/image-20230815210036801.png)

   - 在node02的ZK客户端中`get 锁的节点路径`可以看到，在集群启动后，生成了节点目录，即ZKFC的抢锁机制，在ZK中创建了临时节点，可以看到是node01抢到了锁，是主NN（ephemeralOwner= 表示锁的临时持有者为node01）

     ![image-20230815210336769](assets/image-20230815210336769.png)

   - 浏览器访问，也可看到node01为主NN，node02为备用NN

     <img src="assets/image-20230815212800261.png" alt="image-20230815212800261" style="zoom:67%;" />

     <img src="assets/image-20230815212726060.png" alt="image-20230815212726060" style="zoom:50%;" />

测试主备自动切换

- kill node01中的NN进程 （kill -9 表示强制杀死）

  ![image-20230815212931920](assets/image-20230815212931920.png)

- 在node02的ZK客户端中，再次查看锁，可以看到此时锁的持有者自动切换为node01。浏览器中也可以相应发生主备切换

  ![image-20230815213213018](assets/image-20230815213213018.png)

  ![image-20230815213252518](assets/image-20230815213252518.png)

  <img src="assets/image-20230815213311303.png" alt="image-20230815213311303" style="zoom:67%;" />

- 再次启动node01中的NN（`hadoop-daemon.sh start namenode`），主备不切换

- 杀死node02中的ZKFC进程，此时ZKFC无法连接到ZK集群，主备再次发生切换

  ![image-20230815213553823](assets/image-20230815213553823.png)

- 恢复node02中的ZKFC（`hadoop-deamon.sh start zkfc`），虚拟机环境下将node01的网卡down掉，使其无法与外界通信（`ifconfig eth0 down`），此时两台机器都处于Standby，因为node02此时无法检查到node01的情况，（觉得可能node01只是和自己连不通，还能和外界连接）；检查node02的$HADOOP_HOME/logs 的ZKFC日志（`tail -f hadoop-root-zkfc-node02.log`），可以看到其无法连接到node01

  ![image-20230815214417093](assets/image-20230815214417093.png)

- 再宿主机上恢复node01的网卡通信（`ifconfig eth0 up`），此时成功将node02升为主NN，node01降为Standby，日志也显示成功

  ![image-20230815214610640](assets/image-20230815214610640.png)

  <img src="assets/image-20230815214626527.png" alt="image-20230815214626527" style="zoom:50%;" />



## HDFS命令使用

1. 权限
2. 

# ZooKeeper

在 ZooKeeper 中，节点是数据存储和组织的基本单位。理解 ZooKeeper 中的节点和路径存在关系：

1. **ZooKeeper 服务器集合**: ZooKeeper 是一个分布式系统，通常由多个服务器组成。这些服务器通过共享数据来提供高可用性和容错能力。
2. **树形命名空间**: ZooKeeper 使用一个树形结构来组织和管理数据。这个树形结构被称为命名空间（namespace），类似于文件系统的目录结构。**树的每个节点都可以存储数据**。
3. **ZNode**: 在 ZooKeeper 中，每**个节点被称为 ZNode**。ZNode 是 ZooKeeper 中数据存储的基本单元。**每个 ZNode 都有一个唯一的路径标识，类似于文件系统中的路径**。
   1. **持久节点**: 持久节点是一种在 ZooKeeper 中创建后将一直存在的节点。它们不会因为客户端的断开连接或会话过期而消失。
   2. **临时节点**: 临时节点是在客户端会话存在期间存在的节点。当客户端会话结束时，临时节点将被自动删除。临时节点通常用于临时状态或临时任务的标记。
   3. **顺序节点**: 顺序节点是在节点路径的末尾自动追加一个唯一的递增序列号的节点。顺序节点的创建顺序由 ZooKeeper 服务器保证。顺序节点的序列号使得节点的创建顺序可预测，有助于实现分布式协调和队列等功能。
