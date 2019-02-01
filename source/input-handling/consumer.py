from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from kafka import KafkaProducer
# import psycopg2

PRODUCER = KafkaProducer(bootstrap_servers='10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092')

def handler(message):
    records = message.collect()
    for record in records:
        PRODUCER.send('spark_out', bytes(str(record), 'utf-8'))
        PRODUCER.flush()

def word_counts(kafka_stream):
    lines = kafka_stream.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    return counts

def key_strokes(kafka_stream):
    '''get bits of keystroke and timestamp into digraphs'''
    pass

def main():
    sparkContext = SparkContext(appName = 'testJob')
    sparkContext.setLogLevel('ERROR')
    sparkStreamingContext = StreamingContext(sparkContext, 3)
    kafkaStream = KafkaUtils.createDirectStream(sparkStreamingContext,
            ['hot-topic'],
            {'metadata.broker.list':'10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092'})
    counts = word_counts(kafkaStream)
    counts.pprint()
    counts.foreachRDD(handler)
    
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()

if __name__=="__main__":
    main()
