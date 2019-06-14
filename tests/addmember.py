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


if __name__ == "__main__":

    if len(sys.argv)<3:
        print 'addmember [membernumber],[devicetype("watch"/"band")]'
        exit(0)
    
    mongoconn = pymongo.MongoClient('mongodb://admin:%s@localhost:27017/'%('Kr123$^'))
    db = mongoconn.zhijian
    col = db.departs

    membernumber = int(sys.argv[1])
    devicetype = sys.argv[2]
    r = redis.StrictRedis(password='Kr123456')

    results = col.find()
    passlandlist = list()
    bureaulist = list()
    for obj in results:
        passlandlist.append(obj['passland'])
        bureaulist.append(obj['bureau'])

    index = random.randint(0,len(passlandlist)-1)
    passland = passlandlist[index]
    bureau = bureaulist[index]

    print passland

    if devicetype == 'watch':
        #申请sim，device
        url = 'http://localhost:8081/gearcenter'
        headers = {"Content-type": "application/json"}

        n1 = random.randint(1000,9999999)
        n2 = n1+membernumber-1
        body = dict()
        body['sim_start'] = "139%.8d" % (n1)
        body['sim_end'] = "139%.8d" % (n2)
        body['vid'] = '000001001002'
        body['user']='admin'
        body['simoperator'] = '10086'
        body['passland'] = passland.encode('UTF-8')
        action = dict()
        action['body'] = body
        action['version'] = '1.0'
        action['action_cmd'] = 'sim_add1'
        action['seq_id'] = '%d' % random.randint(0,10000)

        # resp = requests.post(url, data = action)
        # print resp

        http2 = httplib2.Http()
        response, content = http2.request(url, method="POST", body=json.dumps(action))
        print("response =%s" % (response))
        print("conten = %s" % (content))

        #申请device
        body = dict()
        body['imei_start'] = "8600000%.8d" % (n1)
        body['imei_end'] = "8600000%.8d" % (n2)
        body['vid'] = '000001001002'
        body['passland'] = passland.encode('UTF-8')
        body['user']='admin'
        body['devicetype'] = 'watch'
        action = dict()
        action['body'] = body
        action['version'] = '1.0'
        action['action_cmd'] = 'gear_add'
        action['seq_id'] = '%d' % random.randint(0,10000)

        http2 = httplib2.Http()
        response, content = http2.request(url, method="POST", body=json.dumps(action))
        print("response =%s" % (response))
        print("conten = %s" % (content))

        simstart = int("139%.8d" % (n1))
        imeistart = int("8600000%.8d" % (n1))

        url = 'http://localhost:8081/member'
        for i in xrange(0,membernumber):
            body = dict()
            body['username'] = "%d" % (imeistart+i)
            body['vid'] = '000001001002'
            body['simnumber'] = str(simstart+i)
            body['location_timeout'] = 86400
            body['alarminfo'] = 'normal'
            body['fence_name'] = u"\u79bb\u5f00\u6df1\u5733".encode('UTF-8')
            body['photo'] = "http://120.24.160.200:3000/public/images/photos/default.png"
            body['age'] = '24'
            body['codetype'] = u"\u62a4\u7167".encode('UTF-8')
            body['passland'] = passland.encode('UTF-8')
            body['bureau'] = bureau.encode('UTF-8')
            body['deviceid'] = str(imeistart+i)
            body['gender'] = u"\u7537".encode('UTF-8')
            body['user'] = 'admin'
            body['devicetype'] = 'watch'
            body['activateduration'] = 604800
            body['nickname'] =  "%d" % (imeistart+i)
            body['illgroup'] = u"\u57c3\u535a\u62c9".encode('UTF-8')
            body['travel_history'] = 'afdsa'
            body['devicetype'] = 'watch'
            body['devicetype'] = 'watch'
            body['devicetype'] = 'watch'
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'register'
            action['seq_id'] = '%d' % random.randint(0,10000)

            http2 = httplib2.Http()
            response, content = http2.request(url, method="POST", body=json.dumps(action))
            print("response =%s" % (response))
            print("conten = %s" % (content))

#{"body": {"username": "1213131231", "vid": "000001001002", "activetime": "04/28/2017 14:49:00", "simnumber": "222", "location_timeout": 86400, "alarminfo": "normal", "fence_name": "\u79bb\u5f00\u6df1\u5733", "photo": "http://120.24.160.200:3000/public/images/photos/default.png", "age": "24", "codetype": "\u62a4\u7167", "contact_history": "faaa", "passland": "\u7687\u5c97\u53e3\u5cb8", "bureau": "", "deviceid": "333", "gender": "\u7537", "user": "admin", "devicetype": "watch", "activateduration": 604800, "nickname": "test1", "illgroup": "\u57c3\u535a\u62c9", "travel_history": "ffff"}, "from": "W:Queue:HttpProxy:19373", "seq_id": "123", "sockid": "f5a5cb50-2bde-11e7-ac67-00163e003290", "version": "1.0", "action_cmd": "register"}






