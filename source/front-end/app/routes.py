
from kafka import KafkaProducer, KafkaConsumer
from kafka import KafkaClient, SimpleConsumer
bs = ['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092']
PRODUCER = KafkaProducer(bootstrap_servers=bs)
CLIENT = KafkaClient(bs)

import sys
from app import app
from flask import Flask, render_template, request, redirect, Response
import random, json

# app = Flask(__name__)
@app.route('/')
def home():
# serve index template
    return render_template('keylog.html')

@app.route('/new_user')
def initiate():
    return render_template('newuser.html')

@app.route('<user>/new_user', methods = ['POST'])
def complete(user):
    data = request.get_json()
    value = "Incomplete"
    if data:
        with open('{}_start_data.txt'.format(user)) as us:
            us.write(json.dumps(data))
        value = 'Complete'

    return value

@app.route('/<user>/receiver', methods = ['POST'])
def worker(user):
    # read json + reply

    data = request.get_json()
    if data:
        message = '|'.join([f"{user},{user},{dig['k']},{dig['t']}" for dig in data['value']]).replace(' ','Space')
        PRODUCER.send('user_input', bytes(message, 'utf-8'))
    else:
        message = "False"
    return message

@app.route('/<user>/auth')
def authentication(user):
    # consumer = KafkaConsumer('user{}_sess{}'.format(user,user), bootstrap_servers=bs)
    consumer = SimpleConsumer(CLIENT, 'testing', 'user{}_sess{}'.format(user,user))
    # res = consumer.poll(timeout_ms=6000)
    msg = consumer.get_message()
    color='yellow'
    if msg:
        if msg.message.value.decode() == 'True':
            color='green'
        else:
            color='red'
    return render_template('auth_result.html', bgcolor=color)

    # return Response(kafkastream(), mimetype='text/xml')


# def kafkastream(consumer):
#     for msg in consumer:
#         yield('''<html>
#     <head>
#         <title>Am I Authenticating</title>
#     </head>
#     <body>
#         <h1>Message is: ''' + msg.value.decode() + '''</h1>
#     </body>
# </html>msg.value)''')