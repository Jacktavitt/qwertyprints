# from flask import render_template
from flask import Flask, render_template, request, redirect, Response
import random, json
from app import app
from kafka import KafkaProducer

PRODUCER = KafkaProducer( \
    bootstrap_servers=['34.215.198.60:9092','34.217.16.2:9092','18.236.99.206:9092'])

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