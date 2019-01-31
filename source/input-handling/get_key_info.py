# step by step:

# keypress producer:
# connect to incoming data
# the flask ui, but for now can be a spoof
# publish data on topic

# i guess spark will need a consumer for this

# spark will also have a producer to send the results of the test back to us

# auth receiver:
# listen to result topic
# connect to ui
# publish to ui the result

# ===========================================
# sample from 

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
        producer.send('spark.out', str(record))
        producer.flush()

def main():
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 10)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    kvs.foreachRDD(handler)

    ssc.start()
    ssc.awaitTermination()
if __name__ == "__main__":

   main()

