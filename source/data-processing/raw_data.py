#!/usr/bin/python3

# Idea here is to get this unruly bunch of miscreant data to fall in line and get on the same page.
# Here is the ideal Schema for our ML tasks:
# -------------------------------------
# |   User_ID
# -------------------------------------
# |   Session_ID
# -------------------------------------
# |   Key_pair
# -------------------------------------
# |   Digraph_time
# -------------------------------------
# |   Task_id
# -------------------------------------

import argparse


import os
import csv
from pyspark.sql.functions import lit
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *

def split_file_name(file_name):
    user_id = int(file_name[:3])
    session_nbr = int(file_name[3])
    task_id = int(file_name[5])

    return user_id, session_nbr, task_id

# os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"
# os.environ["PYTHONPATH"]="/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.7-src.zip"

BUCKET_NAME = 'u-of-buffalo' 
FILE_NAME = "001001.txt"

# conf = SparkConf().setAppName("MVP_getS3_conf").setMaster("local[*]") ec2-54-214-60-202.us-west-2.compute.amazonaws.com
conf = SparkConf().setAppName("MVP_getS3_conf").setMaster("spark://ec2-54-214-60-202.us-west-2.compute.amazonaws.com:7077")

spark = SparkSession.builder.appName("MVP_getS3_spark").getOrCreate()
# sc = SparkContext(conf=conf)

# schema = StructType([
#         StructField("user_id", IntegerType(), False),
#         StructField("session_id", IntegerType()),
#         StructField("key_pair", StringType()),
#         StructField("digraph_time", IntegerType()),
#         StructField("task_id", IntegerType())
#     ])

# rdd = sc.textFile("s3a://{}/{}.txt".format(BUCKET_NAME, FILE_NAME))
df = spark.read.option("delimiter", " ").csv("s3a://{}/{}".format(BUCKET_NAME, FILE_NAME))
# get info from file name
user_id, _, _ = split_file_name(FILE_NAME)

# not ideal, but a hacky fix.
df_named = df.withColumnRenamed('_c0','key_name')\
    .withColumnRenamed('_c1', 'key_action')\
    .withColumnRenamed('_c2','action_time')\
    .withColumn("user_id", lit(user_id))

df_typed = df_named.withColumn("key_name", df_named["key_name"].cast(StringType())) \
        .withColumn("action_time", df_named["action_time"].cast(IntegerType())) \
        .withColumn("user_id", df_named["user_id"].cast(IntegerType()))

# pgdb_dns = "ec2-34-222-121-241.us-west-2.compute.amazonaws.com"
# pgdb_ip = "34.222.121.241"
# pgdb_port = 5432
# dbname = "keystroke_data"
properties = {
            'user': 'other_user',
            'password': 'KRILLIN',
            'driver': 'org.postgresql.Driver'
        }
mode = 'overwrite'
url = "jdbc:postgresql://34.222.121.241:5432/keystroke_data"
# now write to postgresql
try:
    # df_named.write.option('driver', 'org.postgresql.Driver').jdbc(
    df_typed.write.jdbc(url, 'mvp_dumb', mode=mode, properties=properties)
except Exception as e:
    print( e)






