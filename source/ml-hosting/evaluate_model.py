
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

# SPARK = SparkSession.builder. \
#         appName('t2'). \
#         config("spark.mongodb.input.uri", "mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/models.keystrokes"). \
#         config("spark.mongodb.output.uri", "mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/models.keystrokes"). \
#         getOrCreate()
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
    # lines = kafka_stream.map(lambda x: [x[0], x[1], len(x)])
    lines = kafka_stream.map(lambda x: x[1]) \
        .map(lambda line: Row(line.split('|')))

    # lines.pprint()
    # c1 = lines.map(lambda )
    return lines

if __name__ == "__main__":
    # df = SPARK.read.format("com.mongodb.spark.sql.DefaultSource").load()
    # df.printSchema()
    # retreive user's model
    sparkContext = SparkContext(appName = 'evaluateModels')
    sparkContext.setLogLevel('ERROR')
    sparkStreamingContext = StreamingContext(sparkContext, 3)

    kafkaStream = KafkaUtils.createDirectStream(sparkStreamingContext,
            ['user_input'],
            {'metadata.broker.list':'10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092'})

    keys = form_ml_shape(kafkaStream)

    def process(rdd):

        try:
            spark=getSparkSessionInstance(rdd.context.getConf())

             # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(word=w))
            wordsDataFrame = spark.createDataFrame(rowRdd)
            wordsDataFrame.show()
        except:
            pass

    keys.foreachRDD(process)
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()


    # bst = lgb.Booster(model_file='model.txt')
    # ypred = bst.predict(test_data_mat)`
if __name__=="__main__":
    main()