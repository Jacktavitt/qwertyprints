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

conf = SparkConf().setAppName("MVP_getS3_conf").setMaster("local[*]")
spark = SparkSession.builder.appName("MVP_getS3_spark").getOrCreate()
# sc = SparkContext(conf=conf)

schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("session_id", IntegerType()),
        StructField("key_pair", StringType()),
        StructField("digraph_time", IntegerType()),
        StructField("task_id", IntegerType())
    ])

# rdd = sc.textFile("s3a://{}/{}.txt".format(BUCKET_NAME, FILE_NAME))
df = spark.read.option("delimiter", " ").csv("s3a://{}/{}".format(BUCKET_NAME, FILE_NAME))
# get info from file name
user_id, _, _ = split_file_name(FILE_NAME)

# not ideal, but a hacky fix.
df_named = df.withColumnRenamed('_c0','key_id')\
    .withColumnRenamed('_c1', 'key_action')\
    .withColumnRenamed('_c2','action_time')\
    .withColumn("user_id", lit(user_id))

pgdb_dns = "ec2-34-222-121-241.us-west-2.compute.amazonaws.com"
pgdb_ip = "34.222.121.241"
pgdb_port = 5432
dbname = "keystroke_data"
# now write to postgresql
try:
    df_named.write.jdbc(
        'jdbc:posgresql://{}:{}/{}'.format(pgdb_ip, pgdb_port, dbname),
        'mvp_tester',
        mode='append',
        properties={
            'user': 'other_user',
            'password': 'KRILLIN'
        }
    )
except Exception as e:
    print('used ip ', e)
    try:
        df_named.write.jdbc(
            'jdbc:posgresql://{}:{}/{}'.format(pgdb_dns, pgdb_port, dbname),
            'mvp_tester',
            mode='append',
            properties={
                'user': 'other_user',
                'password': 'KRILLIN'
            }
        )
    except Exception as e:
        print('used dns ', e)





