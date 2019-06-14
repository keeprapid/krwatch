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
import time


if __name__ == "__main__":

    if len(sys.argv)<3:
        print 'addmember [uploadinterval],[uploadcount(0= infinity)]'
        exit(0)
    
    mongoconn = pymongo.MongoClient('mongodb://admin:%s@localhost:27017/'%('Kr123$^'))
    r = redis.StrictRedis(password='Kr123456')
    db = mongoconn.zhijian_member
    col = db.members


    interval = float(sys.argv[1])
    maxcount = int(sys.argv[2])
    maxlat = 415500
    minlat = 208600
    maxlong = 1210500
    minlong = 956600

    members = col.find({'state':1})
    memberlist = list()
    for memberinfo in members:
        memberlist.append(memberinfo)
    sendcount = 0
    if len(memberlist):
        while 1:
            lat = (random.randint(minlat,maxlat))/10000.0
            lng = (random.randint(minlong,maxlong))/10000.0
            index = random.randint(0,len(memberlist)-1)
            memberinfo = memberlist[index]
            action = dict()
            action['action_cmd'] = 'test_gps'
            action['seq_id'] = '123'
            action['version'] = '1.0'
            action['from'] = ''
            body = dict()
            action['body'] = body
            body['imei'] = memberinfo['deviceid']
            body['lng'] = lng
            body['lat'] = lat
            print(action)
            r.lpush("W:Queue:MTPDataCenter",json.dumps(action))
            sendcount +=1
            if maxcount!= 0 and sendcount>maxcount:
                break
            time.sleep(interval)
