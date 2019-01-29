#!/usr/bin/python3

# Idea here is to get this unruly bunch of miscreant data to fall in line and get on the same page.
# Here is the ideal Schema for our ML tasks:
# -------------------------------------
# |   User_ID
# -------------------------------------
# |   Session_ID
# -------------------------------------
# |   Task_id
# -------------------------------------
# |   Digraph_time
# -------------------------------------
# |   Key_pair
# -------------------------------------



import os
import csv
import argparse
import configparser
from boto3 import resource
from pyspark.sql.functions import lit, lag, concat, concat_ws
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column, Window, WindowSpec
from pyspark.sql.types import *

def split_file_name(file_name):
    user_id = int(file_name[:3])
    session_nbr = int(file_name[3])
    task_id = int(file_name[5])
    return user_id, session_nbr, task_id


def main(configfile):

    # read config data
    config = configparser.ConfigParser()
    config.read(configfile)

    conf = SparkConf().setAppName(config['conf']['appname']).setMaster(config['conf']['master'])

    spark = SparkSession.builder.appName(config['conf']['appname']).master(config['conf']['master']).getOrCreate()
    # get list of files in bucket
    s3 = resource('s3')
    bucket = s3.Bucket(config['s3.read']['bucketname'])
    file_list = [file.key for file in bucket.objects.all()]
    for file_name in file_list:
    # iterate over these files and make into schema. TODO: check for bottleneck
        df = spark.read.option("delimiter", " ").csv("s3a://{}/{}".format(config['s3.read']['bucketname'], file_name))
        # get info from file name
        user_id, session_id, task_id = split_file_name(file_name)

        # set column names
        df_named = df.withColumnRenamed('_c0','key_name')   \
            .withColumnRenamed('_c1', 'key_action')     \
            .withColumnRenamed('_c2','action_time')     \
            .withColumn("user_id", lit(user_id))        \
            .withColumn("session_id", lit(session_id))  \
            .withColumn("task_id", lit(task_id))

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
        # model_data.show(12)
        # set up properties
        properties = {
            'user': config['sql.write']['user'],
            'password': config['sql.write']['password'],
            'driver': config['sql.write']['driver']
        }
        try:
            model_data.write.jdbc(
                    config['sql.write']['url'],
                    config['sql.write']['table'],
                    mode=config['sql.write']['mode'],
                    properties=properties
            )
            # print("i think it worked")
        except Exception as e:
            print( e)


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', required=True, help='file containing proper configuration info')
    args = parser.parse_args()
    main(args.config_file)



