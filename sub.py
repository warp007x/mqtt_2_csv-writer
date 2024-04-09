import random
import json
import datetime;
import csv
import os

from paho.mqtt import client as mqtt_client

broker = '3.230.164.113'
port = 1883
topic = "IEMA/WST/34:85:18:B8:2B:D4"
# Generate a Client ID with the subscribe prefix.
client_id = f'iema-client-{random.randint(0, 100)}'
username = 'IEMA@2024'
password = 'Pass@IEMA2024'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # client = mqtt_client.Client(client_id)
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        # print(f"Received `{msg.payload.decode("utf-8")}` from `{msg.topic}` topic")
        # decoded_message=str(msg.payload.decode("utf-8"))
        # message=json.loads(decoded_message)
        # m_in = json.loads(message)
        # print(m_in)
        topic=msg.topic
        m_decode=str(msg.payload.decode("utf-8","ignore"))
        # print("data Received type",type(m_decode))
        print("data Received",m_decode)
        # print("Converting from Json to Object")
        m_in=json.loads(m_decode) #decode json data
        ct = datetime.datetime.now()
        print("current time =", ct)
        # print(type(m_in))
        print("temperature = ",m_in["temperature"])
        print("humidity = ",m_in["humidity"])
        print("pressure = ",m_in["pressure"])
        print("air quality = ",m_in["air_quality"])
        print("PM 2.5 = ",m_in["pm_2_5"])
        print("PM 1.0 = ",m_in["pm_1_0"])
        print("PM 10 = ",m_in["pm_10"])
        print("Wind Speed = ",m_in["wind_speed"])
        print("Wind Direction = ",m_in["wind_direction"])
        list = [str(ct), m_in["temperature"], m_in["humidity"], m_in["pressure"], m_in["air_quality"], m_in["pm_2_5"], m_in["pm_1_0"], m_in["pm_10"], m_in["wind_speed"], m_in["wind_direction"]]
        print(list)
        write_to_csv(list)



    client.subscribe(topic)
    client.on_message = on_message


def write_to_csv(data):
    with open('wst_data.csv', 'a+', newline='') as file:
        writer = csv.writer(file)

        # Check if file is empty (no header)
        file.seek(0)
        first_char = file.read(1)
        if not first_char:
            writer.writerow(["timestamp", "temperature", "humidity", "pressure", "air_quality", "pm_2_5", "pm_1_0", "pm_10", "wind_speed", "wind_direction"])

        writer.writerow(data)  # Write data to CSV
        print('Written to File')



def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()