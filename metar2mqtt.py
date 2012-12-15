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


MQTT_TIMEOUT = 60	#seconds
CLIENT_VERSION = "0.6"
CLIENT_NAME = "metar2mqtt"


#TODO might want to add a lock file
#TODO  need to deal with no config file existing!!!
#TODO move config file to home dir


#read in configuration file
f = file('.metar2mqtt.conf')
cfg = Config(f)
MQTT_HOST = cfg.MQTT_HOST
MQTT_PORT = cfg.MQTT_PORT
CLIENT_TOPIC = cfg.CLIENT_TOPIC
BASE_TOPIC = cfg.BASE_TOPIC
METAR_IDS = cfg.METAR_IDS
INTERVAL = cfg.INTERVAL


mqtt_connected = 0


#define what happens after connection
def on_connect(self, obj, rc):
	global mqtt_connected
	global running
	global alerts

	mqtt_connected = True
	print "MQTT Connected"
	mqttc.publish( CLIENT_TOPIC + "status" , "running", 1, 1 )
	mqttc.publish( CLIENT_TOPIC + "version", CLIENT_VERSION, 1, 1 )
	mqttc.subscribe( CLIENT_TOPIC + "ping", 2)


def on_message(self, obj, msg):
	if (( msg.topic == CLIENT_TOPIC + "ping" ) and ( msg.payload == "request" )):
		mqttc.publish( CLIENT_TOPIC + "ping", "response", qos = 1, retain = 0 )


def do_metar_loop():
	global running
	global METAR_IDS
	global mqttc

	while ( running ):
		if ( mqtt_connected ):
			for station in METAR_IDS:
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

				mqttc.publish(BASE_TOPIC + station + "/time", str(pr.getISOTime()), 1, 1)
				mqttc.publish(BASE_TOPIC + station + "/temperature", str(pr.getTemperatureFahrenheit()), 1, 1)
				if pr.getWindchillF() is not None:
					mqttc.publish(BASE_TOPIC + station + "/windchill", str(pr.getWindchillF()), 1, 1)
				else:
					mqttc.publish(BASE_TOPIC + station + "/windchill", "Unknown", 1, 1)	
				mqttc.publish(BASE_TOPIC + station + "/humidity", str(pr.getHumidity()), 1, 1)
				if pr.getWindSpeed() is not None:
					mqttc.publish(BASE_TOPIC + station + "/wind_speed", str(pr.getWindSpeed()), 1, 1)
				else:
					mqttc.publish(BASE_TOPIC + station + "/wind_speed", "Unknown", 1, 1)
			#	if pr.getWindDirection() is not None:
					mqttc.publish(BASE_TOPIC + station + "/wind_direction", pr.getWindDirection(), 1, 1)
			#	else:
			#		mqttc.publish(BASE_TOPIC + station + "/wind_direction", "Unknown", 1, 1)
				if pr.getWindCompass() is not None:
					mqttc.publish(BASE_TOPIC + station + "/wind_compass", pr.getWindCompass(), 1, 1)
				else:
					mqttc.publish(BASE_TOPIC + station + "/wind_compass", "Unknown", 1, 1)
				if pr.getPressure() is not None:
					mqttc.publish(BASE_TOPIC + station + "/pressure", str(pr.getPressure()), 1, 1)
				else:
					mqttc.publish(BASE_TOPIC + station + "/pressure", "Unknown", 1, 1)
				mqttc.publish(BASE_TOPIC + station + "/dew_point", str(pr.getDewPointFahrenheit()), 1, 1)
				if pr.getCloudtype() is not None:
					mqttc.publish(BASE_TOPIC + station + "/cloud_type", pr.getCloudtype(), 1, 1)
				else:
					mqttc.publish(BASE_TOPIC + station + "/cloud_type", "Unknown", 1, 1)
				mqttc.publish(BASE_TOPIC + station + "/sky_conditions", pr.getSkyConditions(), 1, 1)
			if ( INTERVAL ):
				print "Waiting ", INTERVAL, " minutes for next update."
				time.sleep(60 * INTERVAL)
			else:
				running = False	#do a single shot
				print "Querries complete."
		pass


def do_disconnect():
	   global connected
	   mqttc.disconnect()
	   connected = 0
	   print "Disconnected"


def mqtt_disconnect():
	global mqtt_connected
	print "Disconnecting..."
	mqttc.disconnect()
	if ( mqtt_connected ):
		mqtt_connected = False 
		print "MQTT Disconnected"


def mqtt_connect():

	rc = 1
	while ( rc ):
		print "Attempting connection..."
		mqttc.will_set(CLIENT_TOPIC + "status", "disconnected_", 1, 1)

		#define the mqtt callbacks
		mqttc.on_message = on_message
		mqttc.on_connect = on_connect
#		mqttc.on_disconnect = on_disconnect

		#connect
		rc = mqttc.connect( MQTT_HOST, MQTT_PORT, MQTT_TIMEOUT )
		if rc != 0:
			logging.info( "Connection failed with error code $s, Retrying in 30 seconds.", rc )
			print "Connection failed with error code ", rc, ", Retrying in 30 seconds." 
			time.sleep(30)
		else:
			print "Connect initiated OK"


def cleanup(signum, frame):
	mqtt_disconnect()
	sys.exit(signum)


#create a client
mqttc = mosquitto.Mosquitto( CLIENT_NAME ) 

#trap kill signals including control-c
signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

running = True

t = threading.Thread(target=do_metar_loop)
t.start()


def main_loop():
	global mqtt_connected
	mqttc.loop(10)
	while running:
		if ( mqtt_connected ):
			rc = mqttc.loop(10)
			if rc != 0:	
				mqtt_disconnect()
				print rc
				print "Stalling for 20 seconds to allow broker connection to time out."
				time.sleep(20)
				mqtt_connect()
				mqttc.loop(10)
		pass


mqtt_connect()
main_loop()

