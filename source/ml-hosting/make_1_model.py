import pyspark
import json
import time
import boto3
from boto3 import resource
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from collections import defaultdict
import pymongo
from pymongo import MongoClient
import lightgbm as lgb
import pandas
import QWERTilities as QTS


# Parameters for training
param = {'num_leaves': 31, 'num_trees': 200, 'objective': 'binary',
         'metric': 'auc'}
# number of boosting iterations
num_round = 100

s3 = resource('s3')
# bucket = s3.Bucket('user-ml-models')

spark = SparkSession.builder.getOrCreate()

_start_read_df = time.time()
whole_df = spark.read.csv("s3a://user-keystroke-models/second_data",
                          schema="user_id INT, session_id INT, task_id INT, digraph_time INT, key_pair STRING")
_finish_read_df = time.time()

# TRainiNG MaGick!
_start_pivot = time.time()
pivoted = whole_df.groupBy("user_id", "task_id", "session_id").pivot(
    "key_pair").avg("digraph_time")
    # TODO: get everything in one machine to make this step eaiser?
    # TODO: disinclude taskid?
_finish_pivot = time.time()
feature_df = pivoted.toPandas()
feature_df_users = feature_df.drop(
    ['user_id', 'session_id', 'task_id'], axis=1)
lgbm_eval_dict = defaultdict(list)

all_users = whole_df \
        .select('user_id') \
        .distinct() \
        .rdd \
        .map(lambda r: r[0]) \
        .collect()
# all_users = np.arange(1, 76)
models_put = []

# for user in all_users:
user = 13

_start_user = time.time()
column_names = list(feature_df_users.columns)

temp_df = feature_df[feature_df['session_id'].isin([0, 1])]
train_label = np.array((temp_df['user_id'] == user).values, dtype=int)
train_data_matrix = temp_df.drop(['user_id', 'session_id', 'task_id'], axis=1).as_matrix()
train_data = lgb.Dataset(
    train_data_matrix, label=train_label, feature_name=column_names)

temp_df = feature_df[feature_df['session_id'] == 2]
test_label = np.array((temp_df['user_id'] == user).values, dtype=int)
test_data_matrix = temp_df.drop(
    ['user_id', 'session_id', 'task_id'], axis=1).as_matrix()
test_data = lgb.Dataset(
    test_data_matrix, label=test_label, feature_name=column_names)
_finish_user = time.time()
_start_train = time.time()
bst = lgb.train(param, train_data, num_round)
_finish_train = time.time()
# user_model = bst.dump_model()
tempfn = 'user{}model.txt'.format(user)
bst.save_model(tempfn)
with open(tempfn) as uf:
    text_model = uf.read()

model_package = {}
# user_model = bst.dump_model()
model_package['model'] = text_model
# this will allow us to easily look up the model in MongoDB
model_package['_id'] = user
model_package['test_label'] = test_label.tolist()

item = s3.Object("user-ml-models", "{}_pack.json".format(user))
_start_model_save = time.time()
put_result = item.put(Body=json.dumps(model_package))
_finish_model_save = time.time()

put_result['user_id'] = user
models_put.append(put_result)
# submit_id = collection.insert_one(user_model).inserted_id
print("insterted user {}'s model".format(user))

ts = QTS.make_time_string([
    ('read dataframe time', _start_read_df, _finish_read_df),
    ('pivot time', _start_pivot, _finish_pivot),
    ('user time', _start_user, _finish_user),
    ('training time', _start_train,_finish_train ),
    ('model save time', _start_model_save, _finish_model_save)
])
QTS.write_time_log(ts, 'make_models', models_put)

print("done.\n")

