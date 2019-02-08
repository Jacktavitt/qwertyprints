
import json
import lightgbm as lgb
import pandas as pd
import numpy as np
from sklearn import metrics
import boto3
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

def main():
    sparkContext = SparkContext(appName = 'evaluateModels')
    sparkContext.setLogLevel('ERROR')
    sparkStreamingContext = StreamingContext(sparkContext, 3)
    spark = getSparkSessionInstance(sparkContext.getConf())

    s3 = boto3.resource('s3')
    # boto_client = boto3.client('s3')
    bucket_name = 'user-ml-models'

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

            users = typdf.select('user_id').distinct().rdd.flatMap(lambda x: x).collect()
            for user in users:
                temp = typdf.filter(typdf['user_id']==user)
                # temp.show()
                # TODO: EVALUATE THIS! THURSDAY!
                s3_obj = s3.Object(bucket_name, "{}.json".format(user))
                user_model = json.loads(s3_obj.get()['Body'].read())
                # here we do the pivot into usedul feature matrix 
                pivoted = temp.groupBy("user_id", "session_id") \
                        .pivot("key_pair") \
                        .avg("digraph_time")
                # make it into pandas
                # TODO: duplicate code with make_models.py
                feature_df = pivoted.toPandas()

                the_data_matrix = feature_df.drop(['user_id', 'session_id'], axis=1).as_matrix()
                # load and evaluate the model with lgbm
                bst = lgb.Booster()
                bst.model_from_string(json.dumps(user_model))
                ypred = bst.predict(the_data_matrix)
                # if it passes, send result
                auc = metrics.roc_auc_score(user_model['train_label'], ypred)
                if auc > 50:
                    print('user TRUE')
                else:
                    print('user FALSE')

        except Exception as e:
            print(e)

    keys.foreachRDD(model_user_input)
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()


if __name__=="__main__":
    main()