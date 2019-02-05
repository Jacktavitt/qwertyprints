# from flask import render_template
from flask import Flask, render_template, request, redirect, Response
import random, json
from app import app
from kafka import KafkaProducer

PRODUCER = KafkaProducer( \
    bootstrap_servers=['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092'])

@app.route('/')
def output():
    return render_template('index.html', title="Home")

@app.route('/index/<int:userid>')
def index(userid):
    return render_template('index.html', title="Home", user=userid)

@app.route('/index/receiver', methods = ['POST'])
def worker():
    data = request.get_json()
    res = [str(item) for item in data]
    print(res)
    PRODUCER.send('user_input', bytes(res, 'utf-8'))
    return res

@app.route('/receiver', methods = ['POST'])
def worker():
    # read json + reply
    data = request.get_json()
    result = ''

    for item in data:
    # loop over every row
        result += str(item['make']) + '\n'

    PRODUCER.send('user_input', bytes(result, 'utf-8'))
    return result