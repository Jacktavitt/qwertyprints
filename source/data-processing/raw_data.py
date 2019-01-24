#!/usr/bin/python3

# Idea here is to get this unruly bunch of miscreant data to fall in line and get on the same page.
# Here is the ideal Schema for our ML tasks:
# -------------------------------------
# |   User_ID
# -------------------------------------
# |   Session_ID
# -------------------------------------
# |   Sequence_Num
# -------------------------------------
# |   Key_ID
# -------------------------------------
# |   Duration
# -------------------------------------


def UofBuffaloData(directory):
    pass


import argparse
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column
from pyspark.sql.types import *

os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"

BUCKET_NAME = 'u-of-buffalo' 

conf = SparkConf().setAppName("billy").setMaster("local[*]")
spark = SparkSession.builder.appName("frank").getOrCreate()

#schema didnt work for some reason... BECUASE I DID IT WRONG!
# or did i? All values are 'None' when schema specified.
# schema = StructType([
#     StructField("key_id", StringType(), True),
#     StructField("press_action", StringType(), True),
#     StructField("action_time", IntegerType(), True)])

# schema = StructType([StructField("key_id", StringType(), False),StructField("press_action", StringType(), False),StructField("action_time", IntegerType(), False), StructField("user_id", IntegerType(), False)])

# df = spark.read.option("delimiter", " ").schema(schema).csv("s3a://u-of-buffalo/001001.txt")
df = spark.read.option("delimiter", " ").csv("s3a://u-of-buffalo/001001.txt")
# not ideal, but a hacky fix.
df2 = df.withColumnRenamed('_c0','key_id').withColumnRenamed('_c1', 'key_action').withColumnRenamed('_c2','action_time')

# go through and add columns

sc = SparkContext(conf=conf)
tf = sc.textFile("s3a://u-of-buffalo/001001.txt")

# conf = SparkConf().setAppName("load_data").setMaster("local")
# sc = SparkContext(conf=conf)
# sqlContx = SQLContext(sc)

# uob_bucket = s3.Bucket(BUCKET_NAME)
# files_list = [f.key for f in uob_bucket.objects.all()]

