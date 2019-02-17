from pyspark.sql import SQLContext
from kafka import KafkaProducer
from time import sleep
import argparse
from os import path

def split_file_name(file_name):
    user_id = int(file_name[:3])
    session_id = file_name[:4]
    task_id = int(file_name[5])
    return user_id, session_id, task_id

def main(filename, PRODUCER):
    '''
    pretends to be a user typing, based on raw data from the university of buffalo. Sends over
    a Kafka topic 'synth_user'
    used for testing the spark streaming.
    '''
    with open(filename) as f:
        info = f.read()
    user_id, session_id, _ = split_file_name(path.split(filename)[1])
    splitinfo = [x.split(' ') for x in info.split('\n')]
    base_time = int(splitinfo[0][-1])
    time_list = []

    for line in splitinfo:
        if len(line) == 3 and line[1].lower() == 'keydown':
            tl = [user_id, session_id, line[0], line[1], int(line[2]) - base_time]
            time_list.append(tl)
            base_time = int(line[2])
        
    for entry in time_list:
        sleep(entry[-1]/1000.0)
        PRODUCER.send('user_input', bytes(str(entry), 'utf-8'))
        
if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--filename', required=True,
        help='file of keystrokes to open, sample filename: /home/johnny/Documents/INSIGHT/project/keystrokes/U_of_Buffalo/UB_keystroke_dataset/s0/baseline/002001.txt')
    args = parser.parse_args()
    PRODUCER = KafkaProducer(bootstrap_servers=['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092'])
    main(args.filename, PRODUCER)