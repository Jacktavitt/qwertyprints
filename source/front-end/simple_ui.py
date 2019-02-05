from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello_world():
    return "Hello world"

@app.route('/user/<int:username>')
def get_user_auth(username):
    return f"hello {username}"