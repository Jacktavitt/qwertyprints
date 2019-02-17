# QWERTYprints
#### Continuous Fraud Detection using keystroke-based biometrics
Look at your hands: the length of your fingers, the space between your knuckles. Can you palm a basketball? Barely get your fingers around the wide part of a juice glass? Our hands are one of the first ways we interact with the world around us.
Using a keyboard is one of the major ways we intereact with technology. Are you an index-finger typist, or a typing wizard with unmarked keys? You can tell a lot from how someone types. In fact, research has shown that our typing style can reliably be used as a way to verify our identity.

##### Presentation
https://docs.google.com/presentation/d/14HiPllRAg2pccYvYMaN_n5uLWYFNJ-Zi9zQcuQ4FjKQ/edit?usp=sharing
##### Link
https://bit.ly/2UJrNFK

### Architecture
![Alt text](QWERTYprints_presentation_archi.png?raw=true "Project Architecture")

### Datatsets
![Alt text](https://cubs.buffalo.edu/research/datasets "University of Buffalo Keystroke Identification data")

### Engineering Challenge

Raw data must be manipulated into a useful FEATURE MATRIX. This will have hundreds of columns, one for each pair of keys on a keyboard (M-E, E-T, T-A, A-L, Space-4, 4-Space, L-I, I-F, F-E).
The data is transformed into an intermediate form, of USER | KEY-PAIR | DIGRAPH-TIME for long term storage. This table is loaded and the average digraph time for a key pair is used to interact with the machine learning algorithm.

User models (one per user) are stored in S3. In order to be loaded into the LightGBM algorithm, they must be saved to the spark cluster, and loaded from a file string. The user input data (keystrokes) are sent via Kafka and transformed in a way similar to initial raw data processing. For each user, the data is pivoted, evaluated against the model, and the evaluation returned via Kafka to the Flask server.

### Business Value

The percentage of U.S. adults with a social media account rose from 10% in 2005 to 84% in 2016. Nearly 2 in 3 report being hacked. 11% of those who have been hacked suffered financial loss, or know someone who has. My project involved a PM and DS fellow to develop an app plugin that allows for continuous user authentication. This project trains a machine learning algorithm on customer data, and maintains the trained models with minimal human interference.

### Future Improvements

 -> Automate a method to record evaluation of the models and version them as they are retrained.
 -> Considerable bottleneck for pivoting the tables and converting them to Pandas dataframes for the ML algorithm. Use Parquet to store the feature matrices directly, and a caching system to keep hold of the loaded data models. Test Spark implementation of LightGBM to see if it interacts more naturally.
 -> Kafka has a node API. The front end could be simplified into JavaScript and use the kafka producers/consumers available to provide a more natural user experience and simplify the code
 -> While the Spark Streaming is interesting, the sizes of use input data are small enough that a node with a fast GPU sould be able to serve the ML models in a more efficient way. This would simplify the code and give the data scientist more control over the implementation of the algorithm.

### SETUP
Running 4 node Spark cluster (master and 3 workers), 3 node kafka cluster, and a one-node Flask frontend
raw_data.py:
    does initial processing from S3, and stores back into S3 bucket.
    to run:
    spark-submit --master spark://<<MASTER NODE IP>> --executor-memory 5G --driver-memory 5G --name initial_data_processing source/data-processing/raw_data.py

make_models:
    makes models from each file in the s3 bucket
    to run:
    spark-submit --master spark://<<MASTER NODE IP>> --executor-memory 5G --driver-memory 5G --name make_models source/ml-hosting/make_models.py

    to make_1_model:
    spark-submit --master spark://<<MASTER NODE IP>> --executor-memory 5G --driver-memory 5G --name make_1_model source/ml-hosting/make_1_model.py -u 76

evaluate_models:
    get user input from kafka stream, flips it and reverses it into actionable feature matrix
    spark-submit --master spark://<<MASTER NODE IP>> --executor-memory 5G --driver-memory 5G --name evaluator source/ml-hosting/evaluate_model.py


kafka listener/producer:
    bin/kafka-console-producer.sh --broker-list <<KAFKA BROKER>>:9092 --topic sample_testclear 
    bin/kafka-console-consumer.sh --bootstrap-server  <<KAFKA BROKER>>:9092 --from-beginning --topic sample_testclear

pyspark: 
    pyspark --master spark://<<MASTER NODE IP>> --executor-memory 5G --driver-memory 5G --name pyspark_lab

