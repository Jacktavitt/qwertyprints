from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from kafka import KafkaProducer
# import psycopg2

PRODUCER = KafkaProducer(bootstrap_servers='10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092')

def handler(message):
    records = message # .collect()
    for record in records:
        PRODUCER.send('spark_out', str(record))
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

sparkContext = SparkContext(appName = 'testJob')
sparkContext.setLogLevel('ERROR')
sparkStreamingContext = StreamingContext(sparkContext, 3)



# ['34.215.198.60:9092','34.217.16.2:9092','18.236.99.206:9092']
kafkaStream = KafkaUtils.createDirectStream(sparkStreamingContext,
            ['hot-topic'],
             {'metadata.broker.list':'10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092'})
            # {'metadata.broker.list':'34.215.198.60:9092, 34.217.16.2:9092, 18.236.99.206:9092'})

# kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
counts = word_counts(kafkaStream)
# lines = kafkaStream.map(lambda x: x[1])
# counts = lines.flatMap(lambda line: line.split(" ")) \
#     .map(lambda word: (word, 1)) \
#     .reduceByKey(lambda a, b: a+b)
counts.pprint()
handler(counts)



sparkStreamingContext.start()
sparkStreamingContext.awaitTermination()


