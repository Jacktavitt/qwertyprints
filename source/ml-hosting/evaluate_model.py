
import json
import lightgbm as lgb
import pandas as pd
import numpy as np
from pprint import pprint
from pyspark.sql import Row, SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from kafka import KafkaProducer
from pyspark.sql.types import *
from pyspark.sql.functions import split, trim
import DataSculpting

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def lines_from_stream(kafka_stream):
    lines = kafka_stream.map(lambda x: x[1])
    return lines

if __name__ == "__main__":
    sparkContext = SparkContext(appName = 'evaluateModels')
    sparkContext.setLogLevel('ERROR')
    sparkStreamingContext = StreamingContext(sparkContext, 3)
    spark = getSparkSessionInstance(sparkContext.getConf())

    kafkaStream = KafkaUtils.createDirectStream(sparkStreamingContext,
            ['user_input'],
            {'metadata.broker.list':'10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092'})

    keys = lines_from_stream(kafkaStream)

    def model_user_input(rdd):

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
                # temp.show()
                # TODO: Load user models and see if it can make sense
                # print("loaded user model")
                # user_model.printSchema()

                # here we do the pivot into usedul feature matrix 
                pivoted = temp.groupBy("user_id", "session_id") \
                        .pivot("key_pair") \
                        .avg("digraph_time")
                # make it into pandas
                # TODO: duplicate code with make_models.py
                feature_df = pivoted.toPandas()
                feature_df_users = feature_df.drop(
                        ['user_id', 'session_id', 'task_id'],
                        axis=1)
                column_names = list(feature_df_users.columns)

                train_data = DataSculpting.prepare_feature_matrix(
                    feature_df,
                    user,
                    column_names,
                    [0,1]
                )
                # load and evaluate the model with lgbm
                bst = lgb.Booster(model_file='model.txt')
                ypred = bst.predict(test_data_mat)
                # if it passes, send result

               
        except Exception as e:
            print(e)

    keys.foreachRDD(model_user_input)
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()


if __name__=="__main__":
    main()