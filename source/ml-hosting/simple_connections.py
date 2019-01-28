import pyspark
import json
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('dbreadtest').setMaster("local[*]")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

user_id = 75

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






