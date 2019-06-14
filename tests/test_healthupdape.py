#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
import pymongo
import random
import redis
import requests
import sys
import httplib2
import urllib
import json
import paho.mqtt.client as mqtt
import time


if __name__ == "__main__":

    a = int(sys.argv[1])
    mongoconn = pymongo.MongoClient('mongodb://admin:%s@localhost:27017/'%('Kr123$^'))
    db = mongoconn.zhijian_device
    col = db.devices

    mqttclient = mqtt.Client()
    mqttclient.connect('localhost', int(2883), int(1000))

    devices = col.find()
    memberlist = list()
    for deviceinfo in devices:
        ra = (random.randint(0,1000000))%100
        health = '1'
        if ra < a:
            health = '0'
        else:
            health = '1'
        sendbuf = "INFODEC:%s,%s#" % (deviceinfo['deviceid'],health)
        topic = 'tpicphwatch'
        print(sendbuf)
        mqttclient.publish(topic, sendbuf)
        time.sleep(2)
