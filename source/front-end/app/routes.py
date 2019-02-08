
from kafka import KafkaProducer, KafkaConsumer
bs = ['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092']
PRODUCER = KafkaProducer(bootstrap_servers=bs)


import sys
from app import app
from flask import Flask, render_template, request, redirect, Response
import random, json

# app = Flask(__name__)
@app.route('/')
def home():
# serve index template
    return render_template('keylog.html')


@app.route('/<int:user>/receiver', methods = ['POST'])
def worker(user):
    # read json + reply

    data = request.get_json()
    if data:
        message = '|'.join([f"{user},{user},{dig['k']},{dig['t']}" for dig in data['value']]).replace(' ','Space')
        PRODUCER.send('user_input', bytes(message, 'utf-8'))
    else:
        message = "False"

    consumer = KafkaConsumer('user{}_sess{}'.format(user,user), bootstrap_servers=bs)

    return str([msg.value for msg in consumer])

# @app.route('/<int:user>/auth', methods = ['POST'])
# def auther(user):
#     # read json + reply

#     data = request.get_json()
#     if data:
#         consumer = KafkaConsumer('user{}_sess{}'.format(user,user), bootstrap_servers=bs)
#         message = str([msg.value for msg in consumer])
#     else:
#         message = "False"

#     return message