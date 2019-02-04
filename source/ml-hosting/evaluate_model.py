
import json
# import lightgbm as lgb
import pandas as pd
import numpy as np
import pymongo
from pprint import pprint
from pymongo import MongoClient
from pyspark.sql import Row, SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from kafka import KafkaProducer
from pyspark.sql.types import *
from pyspark.sql.functions import split, trim

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .config("spark.mongodb.input.uri", "mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/models.keystrokes")\
            .config("spark.mongodb.output.uri", "mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/models.keystrokes")\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def form_ml_shape(kafka_stream):
    # TODO: must somehow split this into separate rows!
    lines = kafka_stream.map(lambda x: x[1])
    return lines

if __name__ == "__main__":
    sparkContext = SparkContext(appName = 'evaluateModels')
    sparkContext.setLogLevel('ERROR')
    sparkStreamingContext = StreamingContext(sparkContext, 3)

    spark = getSparkSessionInstance(sparkContext.getConf())
    model_store = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    # model_store.printSchema()

    kafkaStream = KafkaUtils.createDirectStream(sparkStreamingContext,
            ['user_input'],
            {'metadata.broker.list':'10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092'})

    keys = form_ml_shape(kafkaStream)

    def process(rdd):

        try:
            spark=getSparkSessionInstance(rdd.context.getConf())

             # Convert RDD[String] to RDD[Row] to DataFrame
            rows = rdd.flatMap(lambda line: line.split('|'))
            rdf = spark.createDataFrame(rows, StringType())
            spit = split(rdf['value'],',')
            cdf = rdf.withColumn('user_id', spit.getItem(0)) \
                    .withColumn('session_id', spit.getItem(1)) \
                    .withColumn('key', spit.getItem(2)) \
                    .withColumn('duration', spit.getItem(4)) \
                    .drop('value')
            tcdf = cdf.withColumn('duration', trim(cdf['duration']))
            typdf = tcdf.withColumn('duration', tcdf['duration'].cast(LongType())) \
                    .withColumn('user_id', tcdf['user_id'].cast(LongType()))
            # typdf.printSchema()
            # typdf.show(5)
            # split it by user id
            users = typdf.select('user_id').distinct().rdd.flatMap(lambda x: x).collect()
            for user in users:
                temp = typdf.filter(typdf['user_id']==user)
                temp.show()
                # print(type(user), user)
                pipeline = "{{'$match': {{'_id': {}}}}}".format(user)
                user_model = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                        .option("pipeline", pipeline).load()
                print("loaded user mdoel")
                user_model.printSchema()
            # wordsDataFrame = spark.createDataFrame(rowRdd)
            # wordsDataFrame.show()
            # here we do the pivot into usedul feature matrix with pandas
            # load and evaluate the model with lgbm
            # if it passes, send result

            # bst = lgb.Booster(model_file='model.txt')
            # ypred = bst.predict(test_data_mat)`
        except Exception as e:
            print(e)

    keys.foreachRDD(process)
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()


if __name__=="__main__":
    main()