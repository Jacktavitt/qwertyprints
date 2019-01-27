import pymongo
from pymongo import MongoClient

client = MongoClient("mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/test")

db = client['test']
collection = db['mvpModels']

misty = {'wisecracker': 'crow', 'what a cool guy': 'tom'}

sub_id = collection.insert_one(misty).inserted_id

print(sub_id)