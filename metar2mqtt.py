#!/usr/bin/python
# -*- coding: utf-8 -*-
# vim tabstop=4 expandtab shiftwidth=4 softtabstop=4

#
# metar-bridge
#    Provides airport weather data
#


__author__ = "Dennis Sell"
__copyright__ = "Copyright (C) Dennis Sell"


APPNAME = "metar2mqtt"
VERSION = "0.10"


import sys
import os
import mosquitto
import socket
import time
import subprocess
import logging
import signal
import threading
import pymetar
import commands
from daemon import Daemon
from mqttcore import MQTTClientCore
from mqttcore import main


class MyMQTTClientCore(MQTTClientCore):
    def __init__(self, appname, clienttype):
        MQTTClientCore.__init__(self, appname, clienttype)
        self.clientversion = VERSION
        self.metarids = self.cfg.METAR_IDS
        self.interval = self.cfg.INTERVAL
        self.basetopic = self.cfg.BASE_TOPIC
        self.clientversion = VERSION
        self.t = threading.Thread(target=self.do_thread_loop)
        self.t.start()

    def do_thread_loop(self):
        while(self.running):
            if ( self.mqtt_connected ):    
                for station in self.metarids:
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
                    self.mqttc.publish(self.basetopic + station + "/time", str(pr.getISOTime()), qos=1, retain=True)
                    self.mqttc.publish(self.basetopic + station + "/temperature", str(pr.getTemperatureFahrenheit()), qos=1, retain=True)
                    if pr.getWindchillF() is not None:
                        self.mqttc.publish(self.basetopic + station + "/windchill", str(pr.getWindchillF()), qos=1, retain=True)
                    else:
                        self.mqttc.publish(self.basetopic + station + "/windchill", "Unknown", qos=1, retain=True)    
                    self.mqttc.publish(self.basetopic + station + "/humidity", str(pr.getHumidity()), qos=1, retain=True)
                    if pr.getWindSpeed() is not None:
                        self.mqttc.publish(self.basetopic + station + "/wind_speed", str(pr.getWindSpeed()), qos=1, retain=True)
                    else:
                        self.mqttc.publish(self.basetopic + station + "/wind_speed", "Unknown", qos=1, retain=True)
                #    if pr.getWindDirection() is not None:
                        self.mqttc.publish(self.basetopic + station + "/wind_direction", pr.getWindDirection(), qos=1, retain=True)
                #    else:
                #        self.mqttc.publish(self.basetopic + station + "/wind_direction", "Unknown", qos=1, retain=True)
                    if pr.getWindCompass() is not None:
                        self.mqttc.publish(self.basetopic + station + "/wind_compass", pr.getWindCompass(), qos=1, retain=True)
                    else:
                        self.mqttc.publish(self.basetopic + station + "/wind_compass", "Unknown", qos=1, retain=True)
                    if pr.getPressure() is not None:
                        self.mqttc.publish(self.basetopic + station + "/pressure", str(pr.getPressure()), qos=1, retain=True)
                    else:
                        self.mqttc.publish(self.basetopic + station + "/pressure", "Unknown", qos=1, retain=True)
    #TODO need to add pressure trend!!!!!
                    self.mqttc.publish(self.basetopic + station + "/dew_point", str(pr.getDewPointFahrenheit()), qos=1, retain=True)
                    if pr.getCloudtype() is not None:
                        self.mqttc.publish(self.basetopic + station + "/cloud_type", pr.getCloudtype(), qos=1, retain=True)
                    else:
                        self.mqttc.publish(self.basetopic + station + "/cloud_type", "Unknown", qos=1, retain=True)
                    self.mqttc.publish(self.basetopic + station + "/sky_conditions", pr.getSkyConditions(), qos=1, retain=True)
                if ( self.interval ):
                    print "Waiting ", self.interval, " minutes for next update."
                    time.sleep(self.interval*60)


class MyDaemon(Daemon):
    def run(self):
        mqttcore = MyMQTTClientCore(APPNAME, clienttype="type1")
        mqttcore.main_loop()


if __name__ == "__main__":
    daemon = MyDaemon('/tmp/' + APPNAME + '.pid')
    main(daemon)
