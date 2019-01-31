from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
# import psycopg2

sparkContext = SparkContext(appName = 'testJob')
sparkContext.setLogLevel('ERROR')
sparkStreamingContext = StreamingContext(sparkContext, 1)

# ['34.215.198.60:9092','34.217.16.2:9092','18.236.99.206:9092']
kafkaStream = KafkaUtils.createDirectStream(sparkStreamingContext,
            ['hot-topic'],
             {'metadata.broker.list':'10.0.0.12:9092, 10.0.0.8:9092, 10.0.0.7:9092'})
            # {'metadata.broker.list':'34.215.198.60:9092, 34.217.16.2:9092, 18.236.99.206:9092'})

# kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kafkaStream.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a+b)
counts.pprint()

sparkStreamingContext.start()
sparkStreamingContext.awaitTermination()