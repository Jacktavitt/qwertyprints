
# from this site: https://stackoverflow.com/questions/37337086/how-to-properly-use-pyspark-to-send-data-to-kafka-broker

from pyspark import SparkConf, SparkContext
from operator import add
import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def handler(message):
    records = message.collect()
    for record in records:
        producer.send('spark_out', str(record))
        producer.flush()

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
ssc = StreamingContext(sc, 10)

brokers, topic = sys.argv[1:]
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
kvs.foreachRDD(handler)

ssc.start()
ssc.awaitTermination()
