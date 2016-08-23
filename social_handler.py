import paho.mqtt.client as mqtt
import pokemon_list
import mysql.connector
from mysql.connector import IntegrityError
from datetime import datetime, timedelta

cnx = mysql.connector.connect(user='root', password='mysql',
                              host='127.0.0.1',
                              database='dump_mqtt')
cursor = cnx.cursor()


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    if(msg.topic.startswith('pgomapcatch/all/catchable/')):
        topic = msg.topic.split('pgomapcatch/all/catchable/')
        pokemon_id = int(topic[1]) - 1
        pokemon_name = pokemon_list.pokemon[pokemon_id]
        data = msg.payload.split(',')

        current_time = datetime.now() + timedelta(minutes=15)
        time_string = current_time.strftime("%Y-%m-%d %H:%M:%S")

        pokemon = (data[2], '', pokemon_id, data[0], data[1], time_string)
        insert_pokemon(pokemon)

        print("[" + time_string + "] " + pokemon_name + " found at " + data[0] + ", " + data[1])

def insert_pokemon(pokemon):
    add_pokemon = ("INSERT INTO pokemon "
                    "(encounter_id, spawnpoint_id, pokemon_id, latitude, longitude, disappear_time) "
                    "VALUES (%s, %s, %s, %s, %s, %s)")
    try:
        cursor.execute(add_pokemon, pokemon)
    except IntegrityError as e:
        print "Duplicate"

    cnx.commit()


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("test.mosca.io", 1883, 60)
client.subscribe("pgomapcatch/#", 0)

client.loop_forever()