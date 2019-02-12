
from kafka import KafkaProducer, KafkaConsumer
from kafka import KafkaClient, SimpleConsumer
bs = ['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092']
PRODUCER = KafkaProducer(bootstrap_servers=bs)
CLIENT = KafkaClient(bs)

import sys
from app import app
from flask import Flask, render_template, request, redirect, Response
import random, json

@app.route('/')
def home():
    return render_template('setuser.html')

@app.route('/<user>')
def serve_user(user):
    consumer = SimpleConsumer(CLIENT, 'testing', 'user{}_sess{}'.format(user,user))
    msg = consumer.get_message()
    print("received message")
    color='yellow'
    if msg:
        if msg.message.value.decode() == 'True':
            color='green'
        else:
            color='red'
    return render_template('keylog.html', bgcolor=color)

@app.route('/new_user')
def initiate():
    return render_template('newuser.html')

# @app.route('/<user>/new_user', methods = ['POST'])
# def complete(user):
#     data = request.get_json()
#     value = "Incomplete"
#     if data:
#         with open('{}_start_data.txt'.format(user), 'w+') as us:
#             us.write(json.dumps(data))
#         value = 'Complete'

#     return value

@app.route('/<user>/receiver', methods = ['POST'])
def worker(user):
    data = request.get_json()
    if data:
        message = '|'.join([f"{user},{user},{dig['k'].upper()},{dig['t']}" for dig in data['value']]).replace(' ','Space')
        PRODUCER.send('user_input', bytes(message, 'utf-8'))
    else:
        message = "False"
    return message

# @app.route('/<user>/auth')
# def authentication(user):
#     consumer = SimpleConsumer(CLIENT, 'testing', 'user{}_sess{}'.format(user,user))
#     msg = consumer.get_message()
#     color='yellow'
#     if msg:
#         if msg.message.value.decode() == 'True':
#             color='green'
#         else:
#             color='red'
#     return render_template('auth_result.html', bgcolor=color)