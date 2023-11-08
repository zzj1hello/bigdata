import time

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

conf = SparkConf()
# 调整Executor的JVM线程堆栈代销为10Mb
conf.set('spark.driver.extraJavaOptions', '-Xss10m')
spark = SparkSession.builder.appName("test").master('local[1]').config(conf=conf).getOrCreate()
sc = spark.sparkContext

import numpy as np
data = np.loadtxt("rs/user_item_rating.txt") # .astype('float')
data = [(i, j, float(r)) for i, row in enumerate(data) for j, r in enumerate(row)]

schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("itemId", IntegerType(), True),
    StructField("rating", FloatType(), True)
])

ratings = spark.createDataFrame(data, schema=schema)
ratings.show(5)

(training, test) = ratings.randomSplit([1., 0.])

# # Build the recommendation model using ALS on the training data
# # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
# rank为隐藏层大小 regParam为正则项参数 默认是MSEloss
als = ALS(maxIter=100, rank=20, regParam=0.1, userCol="userId", itemCol="itemId", ratingCol="rating",
          coldStartStrategy="drop") # , implicitPrefs=True
print(als.explainParams())
print(als.getUserCol())
model = als.fit(training)
model.userFactors.show(5)
# Evaluate the model by computing the mse on the test data
predictions = model.transform(training)
evaluator = RegressionEvaluator(metricName="mse", labelCol="rating", #mse
                                predictionCol="prediction")
mse = evaluator.evaluate(predictions)
print("MSE = " + str(mse))

# 评分最大的前10个item给every user
userRecs = model.recommendForAllUsers(10)
# 评分最大的前10个user给every item
itemRecs = model.recommendForAllItems(10)

# 为指定user集合 选评分最大的前十个进行推荐
users = ratings.select(als.getUserCol()).distinct().limit(3)
userSubsetRecs = model.recommendForUserSubset(users, 10)
# Generate top 10 user recommendations for a specified set of movies
movies = ratings.select(als.getItemCol()).distinct().limit(3)
movieSubSetRecs = model.recommendForItemSubset(movies, 10)

for row in userSubsetRecs.collect():
    data_res = [(val['itemId'], val['rating']) for val in row['recommendations']]
    print(f"user:{row['userId']}的推荐结果为{data_res}")

for row in movieSubSetRecs.collect():
    data_res = [(val['userId'], val['rating']) for val in row['recommendations']]
    print(f"item:{row['itemId']}的推荐结果为{data_res}")

# time.sleep(1000)