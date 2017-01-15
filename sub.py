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
MQTT_PORT = os.getenv('MQTT_PORT', '1883')
MQTT_HOST = os.getenv('MQTT_HOST', "test.mosquitto.org")

DB_CONNECTION = os.getenv('DB_CONNECTION', "mongodb://localhost:27017/")
MESSENGER_URL = os.getenv('MESSENGER_URL', "http://localhost:3000/incoming")

TOPIC = os.getenv('TOPIC', 'as_demo_mqtt/devices/#')


logging.basicConfig(stream=sys.stdout,
                        level=logging.DEBUG,
                        format='%(filename)s: '
                                '%(levelname)s: '
                                '%(funcName)s(): '
                                '%(lineno)d:\t'
                                '%(message)s')
logger = logging.getLogger(__name__)

try:
    import watchtower
    logger.addHandler(watchtower.CloudWatchLogHandler())
    #set env  to indicate the region export AWS_DEFAULT_REGION=us-east-1
except Exception:
    logger.info('No watchtower')


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


client = MongoClient(DB_CONNECTION)
mydb = client['mqttdemo']

def on_message(mqttc, obj, msg):
    logger.debug("Message received: %s",  msg.topic+" "+str(msg.qos)+" "+msg.payload)

    #save to mongo
    myrecord = {
        "type": "incoming",
        "payload": msg.payload, #TODO this writes out a string. consider parsing json
        "tx": datetime.datetime.utcnow()
    }

    try:
        record_id = mydb.device_messages.insert(myrecord)
        logger.debug ("Inserted record: %s", str(record_id))
    except Exception as e:
        logger.error ("Failed writing mongo record: ", exc_info=True)

    try:
        json_obj = getJson(msg.payload)
        message_out = None
        # IM the fb user if we can
        if not json_obj is None:
            telemetry = json_obj.get('telemetry')
            if not telemetry is None:
                accel = telemetry.get('accel')
                if not accel is None:
                    state = accel.get('state')
                    if not state is None:
                        if state == 'STOPPED':
                            message_out = {"text": "You device is not moving"}
                        elif state == 'MOVING':
                            message_out = {"text": "You device is moving"}
                        else:
                            message_out = {"text":"You device is in limbo"}
                else:
                    loc = telemetry.get('loc')
                    if not loc is None:
                        message_out = {"text": "Your device's location: ({} {})".format(loc.get('lat'), loc.get('long'))}
                        message_out['image_url'] = "https://maps.googleapis.com/maps/api/staticmap?size=764x400&center={},{}&zoom=16&markers={},{}".format(loc.get('lat'), loc.get('long'), loc.get('lat'), loc.get('long'))
                        message_out['item_url'] = "http://maps.apple.com/maps?q={},{}&z=16".format(loc.get('lat'), loc.get('long'))
            if not message_out is None:
                # send update to the user
                url = MESSENGER_URL  # send to messenger

                did = json_obj.get('did')
                data = {"did": did, "message": message_out}
                params = {'access_token': '1234'}
                # TODO handle failure
                try:
                    requests.post(url, params=params, json=data)
                except requests.exceptions.RequestException as e:
                    logger.error("Failed HTTP request: ", exc_info=True)
    except Exception as e:
        logger.error ("Exception: ", exc_info=True)



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
# mqttc = mqtt.Client("Sscriber1")
# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.
mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

# Uncomment to enable debug messages
mqttc.on_log = on_log
#TODO configure params
mqttc.connect(MQTT_HOST, MQTT_PORT, 60)
mqttc.subscribe(TOPIC, 0)

mqttc.loop_forever() #TODO exception handling, unhnadled exceptions
