#!/usr/bin/python3

import os
import csv
import argparse
import configparser
from boto3 import resource
from pyspark.sql.functions import lit, lag, concat, concat_ws
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column, Window, WindowSpec
from pyspark.sql.types import *
from py4j.protocol import Py4JJavaError


def split_file_name(file_name):
    user_id = int(file_name[:3])
    session_nbr = int(file_name[3])
    task_id = int(file_name[5])
    return user_id, session_nbr, task_id

def csv_to_schema(raw_df, file_name):
    user_id, session_id, task_id = split_file_name(file_name)
    df_named = raw_df.withColumnRenamed('_c0','key_name')   \
            .withColumnRenamed('_c1', 'key_action')     \
            .withColumnRenamed('_c2','action_time')     \
            .withColumn("user_id", lit(user_id))        \
            .withColumn("session_id", lit(session_id))  \
            .withColumn("task_id", lit(task_id))
    return df_named

def main():
    '''
    Main function of this picture.
    Grabs files of raw user keystroke data from an S3 bucket, transforms into a more useful schema, and writes back to another S3 bucket.
        sample use:
            spark-submit --master spark://54.68.199.253:7077 --executor-memory 5G --driver-memory 5G source/data-processing/raw_data.py
    '''
    spark = SparkSession.builder.getOrCreate()
    # get list of files in bucket
    s3 = resource('s3')
    bucket = s3.Bucket('u-of-buffalo')
    file_list = [file.key for file in bucket.objects.all()]
    for file_name in file_list:
    # iterate over these files and make into schema. TODO: check for bottleneck
        df = spark.read.option("delimiter", " ").csv("s3a://u-of-buffalo/{}".format(file_name))
        # get info from file name
        user_id, session_id, task_id = split_file_name(file_name)

        # set column names
        df_named = df.withColumnRenamed('_c0','key_name')   \
            .withColumnRenamed('_c1', 'key_action')     \
            .withColumnRenamed('_c2','action_time')     \
            .withColumn("user_id", lit(user_id))        \
            .withColumn("session_id", lit(session_id))  \
            .withColumn("task_id", lit(task_id))

        # df_named = csv_to_schema(df, file_name)

        df_typed = df_named.withColumn("action_time", df_named["action_time"].cast(LongType())) # number is too big! must be LongType instead of IntegerType

        winder = Window.partitionBy("user_id").orderBy("action_time")
        # only use keydowns
        keydowns = df_typed.filter(df_typed["key_action"].isin("KeyDown"))
        # generate digraph time
        timed = keydowns.withColumn("digraph_time", (keydowns["action_time"] - lag(keydowns["action_time"], 1).over(winder)))
        # generate key pairs
        key_prs = timed.withColumn("key_pair", (concat_ws('_', timed["key_name"], lag(timed["key_name"], 1).over(winder))))
        # now drop the columns we dont need
        model_data = key_prs.select("user_id", "session_id", "task_id", "digraph_time", "key_pair")

        try:
            model_data.write.csv("s3a://user-keystroke-models/first_data", mode='append')
        except Py4JJavaError as e:
            print('Encountered the (not terribly helpful) Py4JJavaError. Could not successfully connect to S3 bucket for user {}, session {}.\nError message:\n{}\n'.format(user_id, session_id, e))


if __name__=="__main__":
    main()



