# import pymongo
# from pymongo import MongoClient

# client = MongoClient("mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/test")

# db = client['test']
# collection = db['mvpModels']

# misty = {'wisecracker': 'crow', 'what a cool guy': 'tom'}

# sub_id = collection.insert_one(misty).inserted_id

# print(sub_id)


# from this tutorial: https://www.bogotobogo.com/python/MongoDB_PyMongo/python_MongoDB_RESTAPI_with_Flask.php


from flask import Flask
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo

app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'mvpDumb'
# app.config['MONGO_URI'] = 'mongodb://localhost:27017/restdb'
app.config['MONGO_URI'] = 'mongodb://ec2-52-40-193-219.us-west-2.compute.amazonaws.com:27017/test'

mongo = PyMongo(app)

@app.route('/star', methods=['GET'])
def get_all_stars():
    star = mongo.db.stars
    output = []
    for s in star.find():
        output.append({'name' : s['name'], 'distance' : s['distance']})
    return jsonify({'result' : output})

@app.route('/star/', methods=['GET'])
def get_one_star(name):
    star = mongo.db.stars
    s = star.find_one({'name' : name})
    if s:
        output = {'name' : s['name'], 'distance' : s['distance']}
    else:
        output = "No such name"
    return jsonify({'result' : output})

@app.route('/star', methods=['POST'])
def add_star():
    star = mongo.db.stars
    name = request.json['name']
    distance = request.json['distance']
    star_id = star.insert({'name': name, 'distance': distance})
    new_star = star.find_one({'_id': star_id })
    output = {'name' : new_star['name'], 'distance' : new_star['distance']}
    return jsonify({'result' : output})

if __name__ == '__main__':
    app.run(debug=True)