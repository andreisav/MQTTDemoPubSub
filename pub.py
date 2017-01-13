#!/usr/bin/python
#Web service to publish to MQTT
import sys
import web   #TODO investigate the package
import paho.mqtt.publish as publisher
import os
import datetime
import logging
from pymongo import MongoClient
import json

#TODO configure

MQTT_PORT = os.getenv('MQTT_PORT', '1883')
#os.environ["MQTT_HOST"] = "iot.eclipse.org"
MQTT_HOST = os.getenv('MQTT_HOST', 'test.mosquitto.org')
DB_CONNECTION = os.getenv('DB_CONNECTION', 'mongodb://localhost:27017/')
TOPIC = os.getenv('TOPIC', 'as_demo_mqtt/devices/{}/commands')

logging.basicConfig(stream=sys.stdout,
                        level=logging.DEBUG,
                        format='%(filename)s: '
                                '%(levelname)s: '
                                '%(funcName)s(): '
                                '%(lineno)d:\t'
                                '%(message)s')
logger = logging.getLogger(__name__)

urls = (
        "/commands", "commands",
        )

app = web.application(urls, globals())

class commands:
    def POST(self):
        try:
            query = web.input()
            #TODO validate access_token
            data = getJson(web.data())

            if data is None or data.get('did') is None or data.get('command') is None:
                web.ctx.status = "400 Bad Request"
                return

            #validata data
            # publish to device commands queue
            did = data.get('did')
            if not did == None:
                publishMessage(TOPIC.format(did), json.dumps({"command": data.get('command')}))
            else:
                logger.warning("POST: Invalid Parameters: %s", format(query))
                web.ctx.status = "400 Bad Request"
        except Exception, e:
            logger.error("Exception in POST ", exc_info=True)
            web.ctx.status = "500 Internal Error"


if __name__ == "__main__":
    app.run()  #TODO figure out exception handling


client = MongoClient(DB_CONNECTION)
mydb = client['mqttdemo']

def publishMessage(topic, msg):
    publisher.single(topic, msg, hostname=MQTT_HOST, port=MQTT_PORT)
    # save to mongo
    myrecord = {
        "type": "outgoing",
        "payload": msg,
        "tx": datetime.datetime.utcnow()
    }

    record_id = mydb.device_messages.insert(myrecord)
    logger.debug("Inserted record: %s", str(record_id))

def getJson(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError, e:
        return None
    return json_object