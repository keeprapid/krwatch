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
import datetime


if __name__ == "__main__":

    KEY_TOKEN_NAME_ID = "W:tni:%s:%s:%s"
    KEY_IMEI_ID = "W:g:%s:%s"
    KEY_IMEI_MNAME_MID = "W:im:%s:%s:%s"
    KEY_IMEI_ONLINE_FLAG = "W:g:o:%s"
    KEY_CLIENT_ONLINE_FLAG = 'W:c:o:%s'
    KEY_IMEI_CMD = 'W:im:%s:%s'
    KEY_IMEI_LOCATION = 'W:il:%s'
    KEY_PHONE_VERIFYCODE = 'W:vc:%s:%s'
    KEY_MEMBER_SUVSTATE = "W:msv:%s"
    KEY_MEMBER_LOCSTATE = "W:mlc:%s"
    KEY_MEMBER_HEALTHSTATE = "W:mhs:%s"
    KEY_DEVICEINFO = "K:dinfo:%s"
    KEY_DEVICESUVKEY = "K:dsv:%s"
    KEY_MEMBERINFO = "K:minfo:%s"
    KEY_TOKEN = "K:token:%s"
    KEY_MEMBERHEALTHALARMFILTER = "K:mhaf:%s"
    KEY_MEMBERLOCATIONALARMFILTER = "K:mlaf:%s"
    
    mongoconn = pymongo.MongoClient('mongodb://admin:%s@localhost:27017/'%('Kr123$^'))
    db = mongoconn.zhijian_member
    col = db.members
    r = redis.StrictRedis(password='Kr123456')

    results = col.find()
    for obj in results:
    	memberid = obj['_id'].__str__()
    	updatedict  = dict()
    	updatedict['state'] = 1
    	updatedict['activetime'] = datetime.datetime.now()
        setkey = KEY_DEVICESUVKEY % (obj.get('deviceid'))
        r.set(setkey,'1')
        r.expire(setkey,31*24*60*60)
        setkey = KEY_MEMBER_SUVSTATE % (memberid)
        r.set(setkey,'1')
        r.expire(setkey, obj.get('activateduration'))

        setkey = KEY_MEMBER_LOCSTATE % (memberid)
        r.set(setkey,'1')
        r.expire(setkey, obj.get('location_timeout'))

        setkey = KEY_MEMBER_HEALTHSTATE % (memberid)
        r.set(setkey,'1')
        r.expire(setkey, 24*60*60)

        col.update_one({"_id":obj['_id']},{'$set':updatedict})
        