# QWERTYprints
#### Continuous Fraud Detection using keystroke-based metrics
##### Presentation
https://docs.google.com/presentation/d/14HiPllRAg2pccYvYMaN_n5uLWYFNJ-Zi9zQcuQ4FjKQ/edit?usp=sharing
##### Link
https://bit.ly/2UJrNFK

### Architecture

![Alt text](QWERTYprints_presentation_archi.png?raw=true "Project Architecture")

### Datatsets

University of Buffalo Keystroke Identification data

### Engineering Challenge

One is the challenge to design a custom data model that we will use for the Machine Learning algorithm. I will need to transform the source data sets to comply with the shape of our QWERTYprint schema. The various data sources must all either 
    a) contain the data we need or 
    b) have data that can be expanded into the necessary elements (interperet duration of keystroke from start time and end time, etc.)
I am currently using a vertical layout but am considering the possible advantages of making it wider (a column for each digraph instead of a row for each keypress)

The other challenge is to host a large number of ML trained models (one per user). These models have a TRAIN stage and a TEST stage. TRAIN will take place upon initial batch processing of new data, and TEST will happen when a user is logged in and typing. A stream of data will be sent to the model to authenticate the identity of the user.

### Business Value

The percentage of U.S. adults with a social media account rose from 10% in 2005 to 84% in 2016. Nearly 2 in 3 report being hacked. 11% of those who have been hacked suffered financial loss, or know someone who has. My project involved a PM and DS fellow to develop an app plugin that allows for continuous user authentication. This project trains a machine learning algorithm on customer data, and maintains the trained models with minimal human interference.

### MVP

An INGESTION system that brings in the data STORED in (S3) and sends this data to be TRANSFORMED in (Spark) to match our custom SCHEMA, and STORED in a PostgreSQL RELATIONAL DATABASE. This data is then sent to TRAIN a HOSTED ML model, which is STORED in a DOCUMENT-STORAGE db, such as MongoDB. Small amount of user data (individual user) is evaluated by the trained model, which returns a result (IS user or is NOT user)

### Stretch Goals

Automate a method to record evaluation of the models and version them as they are retrained.

### SETUP!

