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


    imei = sys.argv[1]
    healthid = sys.argv[2]

    action = dict()
    action['action_cmd'] = 'test_health'
    action['seq_id'] = '123'
    action['version'] = '1.0'
    action['from'] = ''
    body = dict()
    action['body'] = body
    body['imei'] = imei
    body['healthid'] = healthid
    print(action)
    r.lpush("W:Queue:MTPDataCenter",json.dumps(action))
    time.sleep(1)
