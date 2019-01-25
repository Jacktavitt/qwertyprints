# QWERTYprints

### Description
#### Continuous Fraud Detection using keystroke-based metrics

### Architecture

(Tentative)

S3 -> Spark -> PostgreSQL -> ML Algorithm (Spark or Container) -> Document KV Storage (MongoDB)
Frontend (User and keystroke info) -> Document KV Storage -> Evaluate Model with ML Algo

### Datatsets

A few, firstly:

University of Buffalo Keystroke Identification data

some others, including:

Touch Screen Phone based keystroke dynamics dataset: http://www.coolestech.com/rhu-keystroke/

Free vs. Transcribed Text for Keystroke-Dynamics Evaluations: http://www.cs.cmu.edu/~keystroke/laser-2012/

Keystroke Dynamics - Benchmark Data Set: https://www.cs.cmu.edu/~keystroke/#sec2

Typing Behavior Dataset: http://cvlab.cse.msu.edu/typing-behavior-dataset.html

MEU-Mobile KSD Data Set: https://archive.ics.uci.edu/ml/datasets/MEU-Mobile+KSD#

these are currently all in different schema layouts.

### Engineering Challenge

One is the challenge to design a custom data model that we will use for the Machine Learning algorithm. I will need to transform the source data sets to comply with the shape of our QWERTYprint schema. The various data sources must all either 
    a) contain the data we need or 
    b) have data that can be expanded into the necessary elements (interperet duration of keystroke from start time and end time, etc.)

The other challenge is to host a large number of ML trained models (most likely one per user) and to automate a system of retraining these models based on new/updated data. These models have a TRAIN stage and a TEST stage. TRAIN will take place upon initial batch processing of new data, and TEST will happen when either
    a) a user is logged in and typing. A MICROBATCH will be sent to the model to authenticate the identity of the user
    b) new BATCH data comes in. This is part of the ML model's EVALUATION stage. If it returns a worng value, it must be RETRAINED.
The automation of this process will pose a challenge both for scheduling and the distributed processing required for the ML aspect.

### Business Value

The percentage of U.S. adults with a social media account rose from 10% in 2005 to 84% in 2016. Nearly 2 in 3 report being hacked. 11% of those who have been hacked suffered financial loss, or know someone who has. My project involved a PM and DS fellow to develop an app plugin that allows for continuous user authentication. This project trains a machine learning algorithm on customer data, and maintains the trained models with minimal human interference.

### MVP

An INGESTION system that brings in the data STORED in (S3) and sends this data to be TRANSFORMED in (Spark) to match our custom SCHEMA, and STORED in a PostgreSQL RELATIONAL DATABASE. This data is then sent to TRAIN a HOSTED ML model, which is STORED in a DOCUMENT-STORAGE db, such as MongoDB. Small amount of user data (individual user) is evaluated by the trained model, which returns a result (IS user or is NOT user)

### Stretch Goals

Automate a method to record evaluation of the models and version them as they are retrained.