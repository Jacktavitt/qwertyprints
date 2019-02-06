
from pyspark.sql.functions import lit, lag, concat, concat_ws
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, Row, Column, Window, WindowSpec
from pyspark.sql.types import *

def csv_to_schema(raw_df, user_id, session_id, task_id):
    '''put csv values, and user generated values 
        into DF with proper data types
    '''
    df_named = raw_df.withColumnRenamed('_c0','key_name')   \
            .withColumnRenamed('_c1', 'key_action')     \
            .withColumnRenamed('_c2','action_time')     \
            .withColumn("user_id", lit(user_id))        \
            .withColumn("session_id", lit(session_id))  \
            .withColumn("task_id", lit(task_id))
    df_typed = df_named.withColumn("action_time", \
            df_named["action_time"].cast(LongType()))
    return df_typed

def window_over_values(winder, dataframe):
    '''go over datafram using window to apply transformation 
        (adding current and last window to create new coumn)
        
        makes digraph names and times
        ARGS:
            winder: pyspark.sql.Window object (the ol' winder over yonder)
            dataframe: pyspark datafram object, stripped of keydowns and named properly
    '''
    timed = dataframe.withColumn("digraph_time", \
            (dataframe["action_time"] - lag(dataframe["action_time"], 1) \
            .over(winder)))
    key_prs = timed.withColumn("key_pair", \
            (concat_ws('_', timed["key_name"], lag(timed["key_name"], 1) \
            .over(winder))))
    return key_prs

