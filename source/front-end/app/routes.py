# from flask import render_template
from flask import Flask, render_template, request, redirect, Response
import random, json
from app import app

@app.route('/')

@app.route('/index/<int:userid>')
def index(userid):
    return render_template('index.html', title="Home", user=userid)

@app.route('/receiver', methods = ['POST'])
def worker():
    data = request.get_json()
    print(str(data))

    return str(data)