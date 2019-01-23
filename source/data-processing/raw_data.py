#!/usr/bin/python3

# Idea here is to get this unruly bunch of miscreant data to fall in line and get on the same page.
# Here is the ideal Schema for our ML tasks:
# -------------------------------------
# |   User_ID
# -------------------------------------
# |   Session_ID
# -------------------------------------
# |   Sequence_Num
# -------------------------------------
# |   Key_ID
# -------------------------------------
# |   Duration
# -------------------------------------


def UofBuffaloData(directory):
    pass

import argparse
from pyspark import SparkContext, SparkConf
import boto3
import botocore

BUCKET_NAME = 'u-of-buffalo' # replace with your bucket name
KEY = '001001.txt' # replace with your object key

s3 = boto3.resource('s3')
resources = s3.get_available_subresources()
print(f"Resources: {resources}\n")
try:
    s3.Bucket(BUCKET_NAME).download_file(KEY, 'my_local_image.jpg')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:
        raise
