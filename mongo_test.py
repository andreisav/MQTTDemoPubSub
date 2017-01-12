#
from pymongo import MongoClient

# client = MongoClient('mongodb://localhost:27017/')
client = MongoClient('mongodb://mqtt_user:password@ds161048.mlab.com:61048/mqttdemo')

# data base name : 'test-database-1'
mydb = client['mqttdemo']

import datetime

myrecord = {
        "author": "Duke",
        "title" : "PyMongo 101",
        "tags" : ["MongoDB", "PyMongo", "Tutorial"],
        "date" : datetime.datetime.utcnow()
        }

record_id = mydb.test.insert(myrecord)

print record_id
print mydb.collection_names()