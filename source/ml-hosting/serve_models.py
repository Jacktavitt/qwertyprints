# This will serve the ML algorithm. Steps:

# for a user:
#     get user's data
#     get some other users' data
#     feed these into the algorithm
#     store result into mongodb
# and somethign else?

# reference: https://spark.apache.org/docs/2.3.0/ml-classification-regression.html

import json
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main():
    """ 
    Summary line. 
  
    Extended description of function. 
  
    Parameters: 
    arg1 (int): Description of arg1 
  
    Returns: 
    int: Description of return value 
  
    """
