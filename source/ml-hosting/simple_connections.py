import pyspark
import json
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lit
import pymongo
from pymongo import MongoClient

# TODO: run this once for each user initially, then once each time new user data is entered.

conf = SparkConf().setAppName('dbreadtest').setMaster("local[*]")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

user_id = 75
default_user_model = "~/qwertyprints/data/user_models.user075_model.json"

# structured_query = "(SELECT * FROM mvp_schema WHERE user_id = '001' LIMIT 1000) foo"
# userdf = spark.sql(structured_query)

whole_df = spark.read \
    .format("jdbc") \
    .option("url","jdbc:postgresql://34.222.121.241:5432/keystroke_data") \
    .option("dbtable", "mvp_schema") \
    .option("user", "other_user") \
    .option("password", "KRILLIN") \
    .option("driver", "org.postgresql.Driver") \
    .load()
# how many distinct keypairs?
whole_df.select(whole_df["key_pair"]).distinct().count()
# user's info'
user_info = whole_df.filter(whole_df['user_id'] == user_id)
# comparison info
# TODO: only grab those values that have the same key_pairs as the user 
# kps = df.select(df['key_pair']).distinct().collect()
# other_users_info = whole_df.filter(whole_df['user_id'] != user_id).filter(whole_df['key_pair'] in kps)
other_users_info = whole_df.filter(whole_df['user_id'] != user_id)

# Now i've got one user's data and data from other users.

# I think i need to label data such that it is true of it is user or false otherwise
label_df = whole_df.withColumn("label", lit(whole_df['user_id'] == user_id))

# TRainiNG MaGick!
# TODO: make a Gradient boost algo here that makes an output
# for now use  JSON file from ../../data/user_models/user075_model.json
with open(default_user_model) as jf:
    user_model = json.load(jf)
# set id so that we can retreive it later
user_model['_id'] = user_id
# create Mongo connection
client = MongoClient("mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/test")
db = client['test']
collection = db['mvpModels']
# insert it into db
submit_id = collection.insert_one(user_model).inserted_id
# maybe assert that submit_id == user_id ?








