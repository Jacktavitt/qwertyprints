import pyspark
import json
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from collections import defaultdict
import pymongo
from pymongo import MongoClient
import lightgbm as lgb
import pandas


# Parameters for training
param = {'num_leaves': 31, 'num_trees': 200, 'objective': 'binary',
         'metric': 'auc'}
# number of boosting iterations
num_round = 100

# create Mongo connection
client = MongoClient(
    "mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/models")
db = client['models']
collection = db['keystrokes']

spark = SparkSession.builder.getOrCreate()

whole_df = spark.read.csv("s3a://user-keystroke-models/first_data",
                          schema="user_id INT, session_id INT, task_id INT, digraph_time INT, key_pair STRING")

# TRainiNG MaGick!
pivoted = whole_df.groupBy("user_id", "task_id", "session_id").pivot(
    "key_pair").avg("digraph_time")
    # TODO: get everything in one machine to make this step eaiser?
    # TODO: disinclude taskid?
feature_df = pivoted.toPandas()
feature_df_users = feature_df.drop(
    ['user_id', 'session_id', 'task_id'], axis=1)

lgbm_eval_dict = defaultdict(list)
all_users = np.arange(1, 76)
for user in all_users:
    column_names = list(feature_df_users.columns)

    temp_df = feature_df[feature_df['session_id'].isin([0, 1])]
    train_label = np.array((temp_df['user_id'] == user).values, dtype=int)
    train_data_matrix = temp_df.drop(
        ['user_id', 'session_id', 'task_id'], axis=1).as_matrix()
    train_data = lgb.Dataset(
        train_data_matrix, label=train_label, feature_name=column_names)

    temp_df = feature_df[feature_df['session_id'] == 2]
    test_label = np.array((temp_df['user_id'] == user).values, dtype=int)
    test_data_matrix = temp_df.drop(
        ['user_id', 'session_id', 'task_id'], axis=1).as_matrix()
    test_data = lgb.Dataset(
        test_data_matrix, label=test_label, feature_name=column_names)

    bst = lgb.train(param, train_data, num_round)
    user_model = bst.dump_model()
    # this will allow us to easily look up the model in MongoDB
    user_model['_id'] = user
    submit_id = collection.insert_one(user_model).inserted_id

print("done.\n")

