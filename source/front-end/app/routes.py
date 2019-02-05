from flask import render_template
from app import app

@app.route('/')

@app.route('/index/<int:userid>')
def index(userid):
    return render_template('index.html', title="Home", user=userid)