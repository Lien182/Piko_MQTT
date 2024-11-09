from enum import IntEnum   
import requests
import json
import paho.mqtt.client as mqtt
import ssl, time, inspect, os
   

piko_url = "192.168.0.11"
broker_address = "brokeraddress" 
broker_user = "user"
broker_password = "password"
topic = "topic"




class dxsentries(IntEnum):   
    DCEingangGesamt = 33556736         # in W
    Ausgangsleistung = 67109120        # in W
    Eigenverbrauch = 83888128          # in W
    #Status
    Status = 16780032                  # 0:Off
    #Statistik - Tag
    Ertrag_d = 251658754               # in Wh
    Hausverbrauch_d = 251659010        # in Wh
    Eigenverbrauch_d = 251659266       # in Wh
    Eigenverbrauchsquote_d = 251659278 # in %
    Autarkiegrad_d = 251659279         # in %
    #Statistik - Gesamt
    Ertrag_G = 251658753               # in kWh
    Hausverbrauch_G = 251659009        # in kWh
    Eigenverbrauch_G = 251659265       # in kWh
    Eigenverbrauchsquote_G = 251659280 # in %
    Autarkiegrad_G = 251659281         # in %
    Betriebszeit = 251658496           # in h
    #Momentanwerte - PV Genertor
    DC1Spannung = 33555202             # in V
    DC1Strom = 33555201                # in A
    DC1Leistung = 33555203             # in W
    DC2Spannung = 33555458             # in V
    DC2Strom = 33555457                # in A
    DC2Leistung = 33555459             # in W
    #Momentanwerte Haus
    HausverbrauchSolar = 83886336      # in W
    HausverbrauchBatterie = 83886592   # in W
    HausverbrauchNetz = 83886848       # in W
    HausverbrauchPhase1 = 83887106     # in W
    HausverbrauchPhase2 = 83887362     # in W
    HausverbrauchPhase3 = 83887618     # in W
    #Netz Netzparameter
    NetzAusgangLeistung = 67109120     # in W
    NetzFrequenz = 67110400            # in Hz
    NetzCosPhi = 67110656
    #Netz Phase 1
    P1Spannung = 67109378              # in V
    P1Strom = 67109377                 # in A
    P1Leistung = 67109379              # in W
    #Netz Phase 2
    P2Spannung = 67109634              # in V
    P2Strom = 67109633                 # in A
    P2Leistung = 67109635              # in W
    #Netz Phase 3
    P3Spannung = 67109890              # in V
    P3Strom = 67109889                 # in A
    P3Leistung = 67109891              # in W


def find_value(request, type):
    first_or_default = next((x for x in response.json()["dxsEntries"] if x["dxsId"] == type), None)
    if first_or_default != None:
        return first_or_default["value"] 
    return 0


def restart():
    import sys
    print("argv was",sys.argv)
    print("sys.executable was", sys.executable)
    print("restart now")

    import os
    os.execv(sys.executable, ['python'] + sys.argv)

#MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT Broker with result: {rc}")
    else:
        print("Failed to connect to Broker, return code = ", rc)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected MQTT Broker disconnection, going to sleeo for 60s, then restart!")
        time.sleep(60)
        print("..restart")
        restart()


client = mqtt.Client( "P1" )

client.tls_set( tls_version=ssl.PROTOCOL_TLSv1_2)
client.username_pw_set(username=broker_user, password=broker_password)
client.tls_insecure_set(False)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.connect( broker_address, 8883, 60 )

api_url_base = f'http://{piko_url}/api/dxs.json?'

api_url = api_url_base

first = True
for s in dxsentries:
    if first:
        api_url += f'dxsEntries={s}'
    else:
        api_url += f'&dxsEntries={s}'    
    first = False


client.loop_start()

while(1):

    response = requests.get(api_url)
    mqtt_msg =  {}

    for s in dxsentries:
        mqtt_msg[s.name] = find_value(response, s.value)

    mqtt_msg_json = json.dumps(mqtt_msg)

    client.publish( topic,  mqtt_msg_json)
    print("Data published!")
    time.sleep(10)

client.loop_stop()

