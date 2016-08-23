import thread
import paho.mqtt.client as mqtt
import Geohash
import errno
from socket import error as socket_error

class MyMQTTClass:
    def __init__(self, clientid=None):
        self._mqttc = mqtt.Client(clientid)
        self._mqttc.on_message = self.mqtt_on_message
    def mqtt_on_message(self, mqttc, obj, msg):
        msg.topic+" "+str(msg.qos)+" "+str(msg.payload)
    def publish(self, channel, message):
        self._mqttc.publish(channel, message)
    def connect_to_mqtt(self):
        self._mqttc.connect("test.mosca.io", 1883, 60)
        self._mqttc.subscribe("pgomapcatch/#", 0)
    def run(self):
        self._mqttc.loop_forever()
class SocialHandler(EventHandler):
    def __init__(self, bot):
        self.bot = bot
        try:
            self.mqttc = MyMQTTClass()
            self.mqttc.connect_to_mqtt()
            thread.start_new_thread(self.mqttc.run)
        except socket_error as serr:
            self.mqttc = None
    def handle_event(self, event, sender, level, formatted_msg, data):
        if self.mqttc == None:
            return
        if event == 'catchable_pokemon':
            if data['pokemon_id']:
                # precision=4 mean 19545 meters, http://stackoverflow.com/questions/13836416/geohash-and-max-distance
                geo_hash = Geohash.encode(data['latitude'], data['longitude'], precision=4)
                self.mqttc.publish("pgomapgeo/"+geo_hash+"/"+str(data['pokemon_id']), str(data['latitude'])+","+str(data['longitude'])+","+str(data['encounter_id']))
                self.mqttc.publish("pgomapcatch/all/catchable/"+str(data['pokemon_id']), str(data['latitude'])+","+str(data['longitude'])+","+str(data['encounter_id']))
