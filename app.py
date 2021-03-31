from flask import Flask, jsonify, request
from instahub_data_import import import_data
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/hello')
def say_hello_world():
    return {'result': "Hello World"}

@app.route('/update', methods = ['POST'])
def fetch_data():
    condition = request.get_json(force=True)
    new_room = condition['room']
    new_fea = condition['fea']
    start_date = condition['startDate']
    end_date = condition['endDate']
   
    data = import_data(new_room,start_date,end_date,new_fea)
    return jsonify(Data=data,Room=new_room, Feature=new_fea)
    # return "tested"


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000,debug=True)