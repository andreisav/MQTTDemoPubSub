#!/usr/bin/python
#MQTT client

import sys
import requests
import os
import json
import datetime
from pymongo import MongoClient
import logging

#TODO configure
os.environ["MQTT_PORT"] = "1883"
#os.environ["MQTT_HOST"] = "iot.eclipse.org"
os.environ["MQTT_HOST"] = "test.mosquitto.org"
os.environ["PORT"] = "4000"
os.environ["HOST"] = "localhost"
os.environ["DB_CONNECTION"] = "mongodb://localhost:27017/"
os.environ["MESSENGER_URL"] = "http://localhost:3000/devices"

logging.basicConfig(stream=sys.stdout,
                        level=logging.DEBUG,
                        format='%(filename)s: '
                                '%(levelname)s: '
                                '%(funcName)s(): '
                                '%(lineno)d:\t'
                                '%(message)s')
logger = logging.getLogger(__name__)


topic = "as_demo_mqtt/devices/#"

try:
    import paho.mqtt.client as mqtt
except ImportError:
    # This part is only required to run the example from within the examples
    # directory when the module itself is not installed.
    #
    # If you have the module installed, just use "import paho.mqtt.client"
    import os
    import inspect
    cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"../src")))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)
    import paho.mqtt.client as mqtt

def on_connect(mqttc, obj, flags, rc):
    logger.info("Connected: %s", str(rc))

client = MongoClient(os.environ["DB_CONNECTION"])
mydb = client['MQTTDemo']

def on_message(mqttc, obj, msg):
    logger.debug("Message received: %s",  msg.topic+" "+str(msg.qos)+" "+str(msg.payload))

    #save to mongo
    myrecord = {
        "type": "incoming",
        "payload": str(msg.payload),
        "tx": datetime.datetime.utcnow()
    }

    try:
        record_id = mydb.device_messages.insert(myrecord)
        logger.debug ("Inserted record: %s", str(record_id))
    except Exception as e:
        logger.error ("Failed writing mongo record: ", exc_info=True)

    json_obj = getJson(msg.payload)

    #IM the fb user if we can
    if not json_obj is None:
        telemetry = json_obj.get('telemetry')
        if not telemetry is None:
            state = telemetry.get('state')
            if not state is None:
               if state == 'STOPPED':
                    message_out = "You device is not moving"
               elif state == 'MOVING':
                    message_out = "You device is moving"
               else:
                    message_out = "You device is in limbo"

        #send update to the user
        url = os.environ["MESSENGER_URL"] #send to messenger

        did = json_obj.get('did')
        data = {"did": did, "msg": message_out}
        params = {'access_token': '1234'}
        #TODO handle failure
        try:
            requests.post(url, params=params, json=data)
        except requests.exceptions.RequestException as e:
            logger.error ("Failed HTTP request: ",  exc_info=True)


def on_publish(mqttc, obj, mid):
    logger.debug("mid: %s", str(mid))

def on_subscribe(mqttc, obj, mid, granted_qos):
    logger.debug("Subscribed: %s %s", str(mid), str(granted_qos))

def on_log(mqttc, obj, level, string):
    logger.debug(string)

def getJson(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError, e:
        return None
    return json_object

# If you want to use a specific client id, use
# mqttc = mqtt.Client("Subscriber1")
# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.
mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

# Uncomment to enable debug messages
#mqttc.on_log = on_log
#TODO configure params
mqttc.connect(os.environ["MQTT_HOST"], os.environ["MQTT_PORT"], 60)
mqttc.subscribe(topic, 0)

mqttc.loop_forever() #TODO exception handling, unhnadled exceptions
