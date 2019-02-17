
import json
import time
import os
import lightgbm as lgb
import pandas as pd
import numpy as np
from sklearn import metrics
import boto3
import random
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import  Window
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from pyspark.sql.types import *
from pyspark.sql.functions import split
from pyspark.sql.functions import trim
from pyspark.sql.functions import lag
from pyspark.sql.functions import concat
from pyspark.sql.functions import concat_ws
import DataSculpting as DSG


def translate_prediction_value(ypred):
    '''
    predicted values form lightGBM are not calibrated. These parameters come from Scott Cole (DS fellow)
    and his investigations into this keystroke dataset.
    '''
    old_pred_samp_valid = np.array([1.89497064e-05, 1.50911086e-03, 3.38746918e-03, 6.98252100e-03,
       1.51188199e-02, 3.30878871e-02, 7.00502972e-02, 1.52832984e-01,
       3.32861811e-01, 9.37748610e-01, 1.00000000e+00])
    new_pred_samp_valid = np.array([0.06924164, 0.92424242, 0.9047619 , 0.94186047, 0.97142857,
       0.98305085, 0.99342105, 0.9875    , 1.        , 0.99816244,
       1.        ])
    valid_thresh = 0.07012
    calib_pred = np.interp(ypred,
                            old_pred_samp_valid, new_pred_samp_valid)
    return calib_pred

def getSparkSessionInstance(sparkConf):
    '''
    get a good spark instance, or the one that is already running.
    '''
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def lines_from_stream(kafka_stream):
    '''
    get the relevant value from the kafka dstream
    '''
    lines = kafka_stream.map(lambda x: x[1])
    return lines

def main():
    sparkContext = SparkContext(appName = 'evaluateModels')
    sparkContext.setLogLevel('ERROR')
    sparkStreamingContext = StreamingContext(sparkContext, 3)
    spark = getSparkSessionInstance(sparkContext.getConf())

    s3 = boto3.resource('s3')
    bucket_name = 'user-ml-models'
    PRODUCER = KafkaProducer(bootstrap_servers=['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092'])
    _STREAM_START = time.time()
    kafkaStream = KafkaUtils.createDirectStream(sparkStreamingContext,
            ['user_input'],
            {'metadata.broker.list':'10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092'})
    keys = lines_from_stream(kafkaStream)

    def model_user_input(rdd):
        '''
        split input into rows, convert to a dataframe. Run a window over it to crate digraph info for key pairs.
        pivot this into a feature matrix for each user that has come in, and load the proper model and evaluate.
        '''
        try:
            _SPARK_START = time.time()
            spark=getSparkSessionInstance(rdd.context.getConf())
            _INPUT_CHOP_START = time.time()
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

            winder = Window.partitionBy("user_id").orderBy("action_time")

            users = typdf.select('user_id').distinct().rdd.flatMap(lambda x: x).collect()
            _INPUT_CHOP_TIME = time.time() - _INPUT_CHOP_START
            _USER_LOOP_START = time.time()
            for user in users:
                # go through and transform user input into a feature matrix like we used to train the models
                # one for each user since this is a party line and data from multiple users come in
                _USERCHOP_START = time.time()
                temp = typdf.filter(typdf['user_id']==user)
                sessions = temp.select('session_id').distinct().rdd.flatMap(lambda x: x).collect()
                timed = temp.withColumn("digraph_time", (temp["action_time"] - lag(temp["action_time"], 1).over(winder)))
                key_prs = timed.withColumn("key_pair", (concat_ws('_', timed["key_name"], lag(timed["key_name"], 1).over(winder))))
                model_data = key_prs.select("user_id", "session_id", "digraph_time", "key_pair")
                pivoted = model_data.groupBy("user_id", "session_id") \
                        .pivot("key_pair") \
                        .avg("digraph_time")
                _USERCHOP_TIME = time.time() - _USERCHOP_START
                _USERPANDA_START = time.time()
                feature_df = pivoted.toPandas()
                _USERPANDA_TIME = time.time() - _USERPANDA_START
                the_data_matrix = feature_df.drop(['user_id', 'session_id'], axis=1).as_matrix()
                _FILEIO_START = time.time()
                # since this implementation of gradient boosting needs to open a file to load a model,
                # we must grab it from s3, save it, and load it again. So streamlined!
                # Might be able to make it work with the spark-specific LightGBM.
                # check to see if the file is already saved to the cluster master.
                # if it is, open it. if not, do the dumb stuff above.
                if not os.path.isfile('{}.txt'.format(user)):
                    s3_obj = s3.Object(bucket_name, "{}.json".format(user))
                    try:
                        user_model = json.loads(s3_obj.get()['Body'].read().decode())
                    except Exception as e:
                        print(e)
                    text_model = user_model['model']
                    with open('{}.txt'.format(user), 'w+') as fw:
                        fw.write(text_model)
                _FILEIO_TIME = time.time() - _FILEIO_START
                _MODEL_START = time.time()
                bst = lgb.Booster(model_file='{}.txt'.format(user))
                # now evaluate
                ypred = bst.predict(the_data_matrix)
                calib_pred = translate_prediction_value(ypred[0])
                result = "{}".format(calib_pred > 0.5)
                # for sess in sessions: 
                _MODEL_TIME = time.time() - _MODEL_START
                PRODUCER.send('user{}_sess{}'.format(user, user), bytes(str(result), 'utf-8'))
                print("user: {} ypred: {} calib_pred: {} result: {} delay: {}".format(user, ypred[0], calib_pred, result, (time.time()-_SPARK_START)))
                print("\n_INPUT_CHOP_TIME: {}\n_USERCHOP_TIME: {}\n_USERPANDA_TIME: {}\n_FILEIO_TIME: {}\n_MODEL_TIME: {}\n" \
                        .format(_INPUT_CHOP_TIME, _USERCHOP_TIME,_USERPANDA_TIME, _FILEIO_TIME, _MODEL_TIME))

        except Exception as e:
            print(e)

    keys.foreachRDD(model_user_input)
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()


if __name__=="__main__":
    main()