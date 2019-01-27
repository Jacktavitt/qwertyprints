
# /pyspark --conf "spark.mongodb.input.uri=mongodb://52.40.193.219/test.mvpModels?readPreference=primaryPreferred" \
#               --conf "spark.mongodb.output.uri=mongodb://52.40.193.219/test.mvpModels" \
#               --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.2


import json
from pyspark.sql import SparkSession



spark = SparkSession \
    .builder \
    .appName("model_to_mongo") \
    .config("spark.mongodb.input.uri", "mongodb://52.40.193.219/test.mvpModels") \
    .config("spark.mongodb.output.uri", "mongodb://52.40.193.219/test.mvpModels") \
    .getOrCreate()

people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
   ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])

people.write.format("com.mongodb.spark.sql").mode("append").save()

def train_model(user_id):
    return {
        "user_id": user_id,
        "model": {
            "blah": "yaddah yaddah"
        }
    }