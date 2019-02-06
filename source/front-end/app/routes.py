
from kafka import KafkaProducer

PRODUCER = KafkaProducer( \
     bootstrap_servers=['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092'])

import sys
from app import app
from flask import Flask, render_template, request, redirect, Response
import random, json

# app = Flask(__name__)
@app.route('/')
def home():
# serve index template
    return render_template('keylog.html')

# @app.route('/<int:user>')
# def output(user):
# # serve index template
#     return render_template('keylog.html')

@app.route('/<int:user>/receiver', methods = ['POST'])
def worker(user):
    # read json + reply
    data = request.get_json()
    if data:
        message = '|'.join([f"{user},{user},{dig['k']},{dig['t']}" for dig in data['value']]).replace(' ','Space')
        PRODUCER.send('user_input', bytes(message, 'utf-8'))
        return message
    else:
        return "FALSE"
