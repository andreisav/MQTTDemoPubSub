#Web service to publish to MQTT
import sys
import web   #TODO investigate the package
import paho.mqtt.publish as publisher
import os
import datetime
import logging
from pymongo import MongoClient

#TODO configure
os.environ["MQTT_PORT"] = "1883"
#os.environ["MQTT_HOST"] = "iot.eclipse.org"
os.environ["MQTT_HOST"] = "test.mosquitto.org"
os.environ["PORT"] = "4000"
os.environ["HOST"] = "localhost"
os.environ["DB_CONNECTION"] = "mongodb://localhost:27017/"

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
            data = web.data()
            #validata data
            # publish to device commands queue
            did = query.get('did')
            if not did == None:
                publishMessage("as_demo_mqtt/devices/commands/" + did, data)
            else:
                logger.warning("POST: Invalid Parameters: %s", format(query))
                web.ctx.status = "400 Bad Request"
        except Exception, e:
            logger.error("Exception in POST ", exc_info=True)
            web.ctx.status = "500 Internal Error"


if __name__ == "__main__":
    app.run()  #TODO figure out exception handling


client = MongoClient(os.environ["DB_CONNECTION"])
mydb = client['MQTTDemo']

def publishMessage(topic, msg):
    publisher.single(topic, msg, hostname=os.environ["MQTT_HOST"], port=os.environ["MQTT_PORT"])
    # save to mongo
    myrecord = {
        "type": "outgoing",
        "payload": msg,
        "tx": datetime.datetime.utcnow()
    }

    try:
        record_id = mydb.device_messages.insert(myrecord)
        logger.debug("Inserted record: %s", str(record_id))
    except Exception as e:
        logger.error ("Failed writing mongo record: ", exc_info=True)
