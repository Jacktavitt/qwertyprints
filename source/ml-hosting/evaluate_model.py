
import json
import lightgbm as lgb
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
import lightgbm as lgb
import pandas
from pyspark.sql import SparkSession

spark = SparkSession.builder. \
        appName('t2'). \
        config("spark.mongodb.input.uri", "mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/models.keystrokes"). \
        config("spark.mongodb.output.uri", "mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/models.keystrokes"). \
        getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()
# retreive user's model

# db = client['test']
# collection = db['mvpModels']

# # format input data


# bst = lgb.Booster(model_file='model.txt')
# ypred = bst.predict(test_data_mat)