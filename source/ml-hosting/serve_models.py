# This will serve the ML algorithm. Steps:

# for a user:
#     get user's data
#     get some other users' data
#     feed these into the algorithm
#     store result into mongodb
# and somethign else?

# reference: https://spark.apache.org/docs/2.3.0/ml-classification-regression.html
# reference: https://github.com/apache/spark/blob/master/examples/src/main/python/ml/gradient_boosted_tree_classifier_example.py

import lightgbm as lgb
from lightgbm.plotting import *

# have df with pivoted tables


# Parameters for training
param = {'num_leaves':31, 'num_trees':200, 'objective':'binary',
         'metric': 'auc'}
# number of boosting iterations
num_round = 100