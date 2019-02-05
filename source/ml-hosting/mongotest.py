import pymongo
from pymongo import MongoClient
from pyspark.sql import SparkSession
import pyspark


spark = SparkSession\
        .builder\
        .getOrCreate()

model_store = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

model_store.printSchema()