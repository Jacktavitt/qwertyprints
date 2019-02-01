from pyspark.sql import SQLContext
from kafka import KafkaProducer
from time import sleep
import argparse
import path

# sample filename: /home/johnny/Documents/INSIGHT/project/keystrokes/U_of_Buffalo/UB_keystroke_dataset/s0/baseline/002001.txt
PRODUCER = KafkaProducer(bootstrap_servers=['34.215.198.60:9092','34.217.16.2:9092','18.236.99.206:9092'])

def split_file_name(file_name):
    user_id = int(file_name[:3])
    session_id = file_name[:4]
    task_id = int(file_name[5])
    return user_id, session_id, task_id

def main(filename, PRODUCER):
    # open file of information
    with open(filename) as f:
        info = f.read()
    user_id, session_id, _ = split_file_name(filename)
    splitinfo = [x.split(' ') for x in info.split('\n')]
    base_time = int(splitinfo[0][-1])
    time_list = []
    for index, line in enumerate(splitinfo):
        if len(line) == 3 and line[1].lower() == 'keydown':
            tl = [user_id, session_id, line[0], line[1], int(line[2]) - base_time]
            time_list.append(tl)
            base_time = int(line[2])
        
    for entry in time_list:
        sleep(entry[-1]/1000.0)
        PRODUCER.send('synth_user', bytes(str(entry), 'utf-8'))
        
if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument('-f', '--filename', required=True,
        help='file of keystrokes to open')
    PRODUCER = KafkaProducer(bootstrap_servers=['34.215.198.60:9092','34.217.16.2:9092','18.236.99.206:9092'])
