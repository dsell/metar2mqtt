#!/usr/bin/python
# -*- coding: utf-8 -*-
# vim tabstop=4 expandtab shiftwidth=4 softtabstop=4

#
# metar-bridge
#	Provides airport weather data
#


__author__ = "Dennis Sell"
__copyright__ = "Copyright (C) Dennis Sell"



import sys
import mosquitto
import socket
import time
import subprocess
from gi.repository import NetworkManager, NMClient
import logging
import signal
import pynotify
import threading
from config import Config
import pymetar


MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TIMEOUT = 60	#seconds
CLIENT_VERSION = "0.4"

timer = 10000
client_name = "metar-bridge"
connected = 0

station = sys.argv[1]
print ">" + station


#define what happens after connection
def on_connect(self, obj, rc):
	global connected
	global timer

	connected = 1
	print "Connected"
	mqttc.publish ( "/clients/metar-bridge/status" , "running", 1, 1 )
	mqttc.publish("/clients/metar-bridge/version", CLIENT_VERSION, 1, 1)

	try:
	    rf=pymetar.ReportFetcher(station)
	    rep=rf.FetchReport()
	except Exception, e:
	    sys.stderr.write("Something went wrong when fetching the report.\n")
	    sys.stderr.write("These usually are transient problems if the station ")
	    sys.stderr.write("ID is valid. \nThe error encountered was:\n")
	    sys.stderr.write(str(e)+"\n")
	    sys.exit(1)

	rp=pymetar.ReportParser()
	pr=rp.ParseReport(rep)

	mqttc.publish("/raw/metar/kamw/time", str(pr.getISOTime()), 1, 1)
	mqttc.publish("/raw/metar/kamw/temperature", str(pr.getTemperatureFahrenheit()), 1, 1)
	if pr.getWindchillF() is not None:
		mqttc.publish("/raw/metar/kamw/windchill", str(pr.getWindchillF()), 1, 1)
	else:
		mqttc.publish("/raw/metar/kamw/windchill", "Unknown", 1, 1)	
	mqttc.publish("/raw/metar/kamw/humidity", str(pr.getHumidity()), 1, 1)
	if pr.getWindSpeed() is not None:
		mqttc.publish("/raw/metar/kamw/wind_speed", str(pr.getWindSpeed()), 1, 1)
	else:
		mqttc.publish("/raw/metar/kamw/wind_speed", "Unknown", 1, 1)
#	if pr.getWindDirection() is not None:
		mqttc.publish("/raw/metar/kamw/wind_direction", pr.getWindDirection(), 1, 1)
#	else:
#		mqttc.publish("/raw/metar/kamw/wind_direction", "Unknown", 1, 1)
	if pr.getWindCompass() is not None:
		mqttc.publish("/raw/metar/kamw/wind_compass", pr.getWindCompass(), 1, 1)
	else:
		mqttc.publish("/raw/metar/kamw/wind_compass", "Unknown", 1, 1)
	if pr.getPressure() is not None:
		mqttc.publish("/raw/metar/kamw/pressure", str(pr.getPressure()), 1, 1)
	else:
		mqttc.publish("/raw/metar/kamw/pressure", "Unknown", 1, 1)
	mqttc.publish("/raw/metar/kamw/dew_point", str(pr.getDewPointFahrenheit()), 1, 1)
	if pr.getCloudtype() is not None:
		mqttc.publish("/raw/metar/kamw/cloud_type", pr.getCloudtype(), 1, 1)
	else:
		mqttc.publish("/raw/metar/kamw/cloud_type", "Unknown", 1, 1)
	mqttc.publish("/raw/metar/kamw/sky_conditions", pr.getSkyConditions(), 1, 1)
	timer = 20


def do_disconnect():
       global connected
       mqttc.disconnect()
       connected = 0
       print "Disconnected"


#create a client
mqttc = mosquitto.Mosquitto( client_name ) 

mqttc.will_set("/clients/metar-bridge/status", "done", 1, 1)

#define the callbacks
mqttc.on_connect = on_connect


#connect
rc = mqttc.connect( MQTT_HOST, MQTT_PORT, MQTT_TIMEOUT )
print "waiting for message to pass"
if rc == 0:
	mqttc.loop()
while  timer > 0:
			rc = mqttc.loop()
#			print rc
#			print connected
			if rc != 0:
				do_disconnect()
				pass
			pass
			timer = timer - 1
#			print timer
mqttc.publish("/clients/metar-bridge/status", "done", 1, 1)
mqttc.loop()
mqttc.loop()
mqttc.disconnect()
print "disconnected"


