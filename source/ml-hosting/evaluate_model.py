
import json
import lightgbm as lgb
import pandas as pd
import numpy as np
from sklearn import metrics
import boto3
import random
from pyspark.sql import Row, SparkSession, Window
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from pyspark.sql.types import *
from pyspark.sql.functions import split, trim
import DataSculpting as DSG

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
    PRODUCER = KafkaProducer(bootstrap_servers=['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092'])
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
                    .withColumn('key_name', spit.getItem(2)) \
                    .withColumn('action_time', spit.getItem(3)) \
                    .drop('value')
            tcdf = cdf.withColumn('action_time', trim(cdf['action_time']))
            typdf = tcdf.withColumn('action_time', tcdf['action_time'].cast(LongType())) \
                    .withColumn('user_id', tcdf['user_id'].cast(LongType()))

            by_one_window = Window.partitionBy("user_id").orderBy("action_time")

            users = typdf.select('user_id').distinct().rdd.flatMap(lambda x: x).collect()
            # users.remove(None)
            for user in users:
                temp = typdf.filter(typdf['user_id']==user)
                sessions = temp.select('session_id').distinct().rdd.flatMap(lambda x: x).collect()
                key_prs = DSG.window_over_values(by_one_window, temp)
                model_data = key_prs.select("user_id", "session_id", "digraph_time", "key_pair")
                pivoted = model_data.groupBy("user_id", "session_id") \
                        .pivot("key_pair") \
                        .avg("digraph_time")
                feature_df = pivoted.toPandas()
                s3_obj = s3.Object(bucket_name, "{}_pack.json".format(user))
                user_model = json.loads(s3_obj.get()['Body'].read().decode())
                # test_label = np.array(user_model['test_label'])

                the_data_matrix = feature_df.drop(['user_id', 'session_id'], axis=1).as_matrix()

                text_model = user_model['model']
                with open('temp.txt', 'w+') as fw:
                    fw.write(text_model)
                bst = lgb.Booster(model_file='temp.txt')
                # now evaluate
                ypred = bst.predict(the_data_matrix)
                # auc = metrics.roc_auc_score(test_label, ypred)
                # result = auc > 50
                result = ypred
                print("user: {} result: {}".format(user, result))
                for sess in sessions:
                    PRODUCER.send('user{}_sess{}'.format(user, sess), bytes(str(result), 'utf-8'))
        except Exception as e:
            print(e)

    keys.foreachRDD(model_user_input)
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()


if __name__=="__main__":
    main()