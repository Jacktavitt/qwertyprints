# from flask import render_template
from flask import Flask, render_template, request, redirect, Response
import random, json
from app import app
from kafka import KafkaProducer

PRODUCER = KafkaProducer( \
    bootstrap_servers=['54.218.73.149:9092','50.112.197.74:9092','34.222.135.111:9092'])

@app.route('/')

@app.route('/index/<int:userid>')
def index(userid):
    return render_template('index.html', title="Home", user=userid)

@app.route('/receiver', methods = ['POST'])
def worker():
    data = request.get_json()
    print(str(data))
    PRODUCER.send('user_input', bytes(str(data), 'utf-8'))
    return str(data)
