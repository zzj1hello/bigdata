from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').master('local[*]')\
            .config("spark.sql.warehouse.dir", 'hdfs://localhost:9000/user/hive/warehouse')\
            .config("hive.metastore.uris", 'thrift://localhost:9083')\
            .enableHiveSupport().getOrCreate()
sc = spark.sparkContext

spark.sql("select * from sparkonhive").show()
