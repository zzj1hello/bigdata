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



## 架构

客户端

- 通过Hive shell命令行连接；JDBC/ODBC，通过JAVA访问（需要通过Thrift中间件）；比较少会通过浏览器web接口访问

元数据库 metastore

- 元数据包括表名、表所属数据库（默认数据库为default）、表的拥有者、列/分区字段、表的类型（是否为外部表）、表数据的所在目录（外部表的数据存在HDFS中，需给出HDFS中的目录；内部表存在Hive本地目录中）

  >关系型数据库中的数据表类型主要有以下几种：
  >
  >1. 普通表（Regular Table）：这是最常见的表类型，用于存储和组织数据。普通表由行和列组成，每一行表示一个记录，每一列表示一个数据字段。
  >2. 临时表（Temporary Table）：临时表是在会话期间临时创建的表，用于存储临时数据或中间结果。它们可以在会话结束时自动销毁。
  >3. 视图（View）：视图是基于一个或多个表的查询结果，以虚拟表的形式呈现。视图不存储实际的数据，而是根据定义时的查询动态生成数据。
  >4. 分区表（Partitioned Table）：分区表将数据按照某个列的值进行分区，将数据分散存储在不同的分区中。这样可以**提高查询性能和管理大型数据集的效率**。
  >5. 分桶表（Bucketed Table）：分桶表是将数据分散存储在固定数量的桶（buckets）中。数据通过分桶函数映射到不同的桶中，每个桶存储一部分数据。分桶表通常是为了**提高查询的并行度**，而不是为了数据组织或过滤。
  >6. 外部表（External Table）：外部表是关系型数据库中的一种特殊类型，它与数据库管理系统的存储引擎解耦。外部表的数据存储在数据库之外，例如在文件系统或对象存储中。数据库通过外部表与外部数据进行关联和查询，而无需将数据复制到数据库内部。

- 一般采用MySQL存放元数据和内部表数据

Hive的Driver

- 解析器（SQL Parser）：将SQL字符串转成抽象语法树**AST**，检查语法问题、表是否存在、字段是否存在。一般是通过第三方工具库完成，如ANTLR
- 编译器（Physical Plan）：将AST编译生成**逻辑执行计划**
- 优化器（Query Optimizer）：对逻辑执行计划进行优化（选择右表，是否缓存，是否拉取外部数据...）
- 执行器（Execution）：把逻辑执行计划转成MR/Spark（优先会看能不能转成Spark计算引擎）

<img src="assets/image-20230902231219136.png" alt="image-20230902231219136" style="zoom:50%;" />

## Hive源码编译

以Hive on Spark环境为例，学习Hive的源码编译和组件中间的兼容性问题如何解决

- Hive和Hadoop兼容
- Hive和Spark兼容
- 使用git/github、maven进行源码编译

1. 安装Hive和Hadoop环境，选择版本都为3.1.3

    https://hive.apache.org/general/downloads/ Hive官网显示：This release works with Hadoop 3.x.y

   集群规划：

   ![image-20230902113116819](assets/image-20230902113116819.png)

   ![image-20230902113135341](assets/image-20230902113135341.png)

   - Hadoop环境没有问题，可以进行MR运算

   - 启动Hive客户端报错，`NoSuchMethodError`存在版本冲突。可以查到这是`Guava`类库函数调用

     ![image-20230902113311069](assets/image-20230902113311069.png)

   - 进入Github中Hive3.1.3版本的仓库，在`pom.xml`中查看到`guava`类的引入版本为19.0` <guava.version>19.0</guava.version>`；而在Hadoop3.1.3版本的仓库中，在`hadoop/hadoop-project/pom.xml`中可以看到引入的`guava`类版本为` <guava.version>27.0-jre</guava.version>`。故可以猜测存在版本的不一致，导致存在低版本的`guava`方法与高版本的不兼容

     <table>
       <tr>
         <td>
           <img src="assets/image-20230902133659492.png" alt="Image 1">
         </td>
         <td>
           <img src="assets/image-20230902133847214.png" alt="Image 2">
         </td>
       </tr>
     </table>

   - 而在Hadoop3.1.2的对应`guava`类版本为` <guava.version>11.0.2</guava.version>`；在Hadoop3.1.3的提交记录中可以看到，这个大版本的更新是连带issue被解决的

     ![image-20230902141009022](assets/image-20230902141009022.png)

   - 选择升级3.1.3中Hive的guava版本，而不是降级Hadoop为3.1.2，承受漏洞风险

2. 拉取Hive源码，配置环境

   - 需要在Linux上编译Hive源码，并为了方便使用idea maven的图形化构建工具，装一个Linux的GUI

   - 装上JDK、maven、idea、git、

   - 在GUI中打开一个终端，在idea安装目录下启动idea `/bin/idea.sh`，配置maven

   - 选择“Get from VCS”，从版本控制系统获取 `https://github.com/apache/hive.git`或者国内的`https://gitee.com/apache/hive.git`

     <img src="assets/image-20230902154101387.png" alt="image-20230902154101387" style="zoom:50%;" />

   - 切换到指定分支，并创建自己的分支

     <table>
       <tr>
         <td>
           <img src="assets/image-20230902154942215.png" alt="Image 1">
         </td>
         <td>
           <img src="assets/image-20230902160228913.png" alt="Image 2">
         </td>
       </tr>
     </table>

   - 在官网找到使用maven进行编译打包的命令 `mvn clean package -Pdist -DskipTests -Dmaven.javadoc.skip=true`，在idea的Terminal终端中进入maven根目录运行。可以成功编译当前版本下的maven

     <img src="assets/image-20230902155816903.png" alt="image-20230902155816903" style="zoom:50%;" />

3. 修改源码，重新打jar包，解压替换原来的Hive目录

   - 在`pom.xml`中修改`guava.version`， 再重新加载依赖

     ```xml
     <guava.version>19.0</guava.version>
     <guava.version>27.0-jre</guava.version>
     ```

   - 此时再进行编译打包，会发生报错

   - 根据官网的函数描述可能要改很多过时的函数，修改很麻烦

   - 选择使用补丁文件，git apply该补丁文件进行一次性修改；或者点击idea中的交互界面

     <img src="assets/image-20230902162457300.png" alt="image-20230902162457300" style="zoom:50%;" />

   - 可以成功编译打包，将安装包解压到原来的Hive安装目录中，修改目录名，复用环境变量，将原来Hive目录下的配置文件`hive-site.xml`和MySQL驱动jar包（使能访问MySQL中的元数据信息），cp到当前Hive目录中

     ![image-20230902162553497](assets/image-20230902162553497.png)

     ```bash
     tar -zxvf apache-hive-3.1.3-bin.tar.gz -C /opt/module/
     mv hive hiveback
     mv apache-hive-3.1.3-bin hive
     cd hive
     cp /opt/module/hiveback/conf/hive-site.xml ./conf/
     cp /opt/module/hiveback/lib/mysql-connector-java-5.1.27-bin.jar ./lib/
     ```

   - 此时能成功进入到Hive的客户端

     <img src="assets/image-20230902163254124.png" alt="image-20230902163254124" style="zoom:50%;" />

4. 直接进行数据表的创建，记录的插入都能成功，这是因为Hive客户端默认是直接通过JDBC连接到MySQL，获取元数据没出错的

5. 修改连接到MySQL的方式，通过metastore服务作为中间件连接，此时会出现异常报错

   - 修改metastore服务相关的配置项，设置访问metastore服务的地址

     ```xml
     <property>
         <name>hive.metastore.uris</name>
         <value>thrift://hadoop102:9083</value> 
         <description>Thrift URI for the remote metastore. Used by metastore client to connect to remote metastore.
         </description>l 
     </property>
     ```

   - 在一台hadoop102作为Hive客户端，另一台hadoop102启动metastore服务

     ```bash
     hive --service metastore # 本地启动metastore服务
     hive # 启动客户端
     hive (default)> insert into table student values(1, 'abc')
     ```

     - 记录能插入，**但统计任务失败**

       ![image-20230902165243003](assets/image-20230902165243003.png)

       ![image-20230902165327965](assets/image-20230902165327965.png)

   - 查看日志`tail -500 /tmp/用户名/hive.log`，关键是`ClassCastException`，导致的统计任务失败

     <img src="assets/image-20230902165433418.png" alt="image-20230902165433418" style="zoom:33%;" />

6. 在apache官网上可以找到该问题所提issue以被解决，列出了修复好的Hive版本

   <img src="assets/image-20230902170143097.png" alt="image-20230902170143097" style="zoom: 33%;" />

   - 可以直接在idea的git中找到历史提交记录，搜索issue号，选择最新的提交记录，和上面一样通过生成patch，导入的方法修复问题

     <img src="assets/image-20230902194214484.png" alt="image-20230902194214484" style="zoom:50%;" />

   - 在idea中可以有更便捷的做法，直接选择"Cherry-Pick摘樱桃"就能达到打补丁再导入的效果

     ![image-20230902194439138](assets/image-20230902194439138.png)

   - 重新编译打包，再解压到Hive目录中，设置metastore服务连接MySQL，测试insert语句，发现没有问题

7. 为Hive配置Spark引擎

   - 配置3.1.3版本的Spark环境，在Hive创建Spark的配置文件

   - 执行insert语句，发现报错`ClassNotFoundException`，猜测是Hive和Spark版本不兼容，Spark没有`AccumulatorParam`类

     ![image-20230902194918301](assets/image-20230902194918301.png)

     <img src="assets/image-20230902195417680.png" alt="image-20230902195417680" style="zoom:50%;" />

     <img src="assets/image-20230902195441515.png" alt="image-20230902195441515" style="zoom:50%;" />

8. 将Hive源码中对Spark的依赖升级为3.1.3，再编译打包，发布到Hive目录

   - 修改Hive源码中的`pom.xml`，修改Spark版本号，需要同步修改Scala版本，否则后来者会不兼容，maven重新加载依赖

     ```xml
     <spark.version>3.1.3</spark.version><scala.binary.version>2.12</scala.binary.version><scala.version>2.12.10</scala.version>
     ```

   -  同样使用patch补丁文件，apply patch，再重新编译打包，发布到Hive目录，执行insert

     ![image-20230902200050869](assets/image-20230902200050869.png)

     