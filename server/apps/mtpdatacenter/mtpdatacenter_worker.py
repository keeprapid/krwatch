#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  gearcenter_worker.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo gearcenter 工作线程

import sys
import subprocess
import os
import time
import datetime
import time
import threading
import json
import pymongo
import logging
import logging.config
import uuid
import redis
import hashlib
import urllib
import base64
import random
from bson.objectid import ObjectId
if '/opt/Keeprapid/KRWatch/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/KRWatch/server/apps/common')
import workers
from Crypto.Cipher import AES
import struct
import array


BS = AES.block_size
pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
unpad = lambda s : s[0:-ord(s[-1])]

logging.config.fileConfig("/opt/Keeprapid/KRWatch/server/conf/log.conf")
logger = logging.getLogger('krwatch')


class MemberLogic(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(MemberLogic, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("MemberLogic :running in __init__")

        fileobj = open("/opt/Keeprapid/KRWatch/server/conf/mqtt.conf", "r")
        self._mqttconfig = json.load(fileobj)
        fileobj.close()

        fileobj = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:MTPDataCenter"
        if 'mtpdatacenter' in _config:
            if 'Consumer_Queue_Name' in _config['mtpdatacenter']:
                self.recv_queue_name = _config['mtpdatacenter']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.zhijian_member
        self.collect_memberinfo = self.db.members
        self.collect_memberlog = self.db.memberlog
#        self.gearconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.gearconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbgear = self.gearconn.zhijian_device
        self.collect_gearinfo = self.dbgear.devices
        self.collect_siminfo = self.dbgear.sims
#        self.loconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.loconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.locdb = self.loconn.location
        self.collect_gpsinfo = self.locdb.gpsinfo
        self.collect_lbsinfo = self.locdb.lbsinfo

        self.zhijianconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.zhijiandb = self.zhijianconn.zhijian
        self.col_operatelogs = self.zhijiandb.operatelogs

        self.infoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.infodb = self.infoconn.info
        self.collect_infotemp = self.infodb.infotemp
        self.collect_infostate = self.infodb.infostate
        self.collect_infolog = self.infodb.infolog

        self.dataconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.datadb = self.dataconn.zhijian_data
        self.collect_bodyfunction = self.datadb.bodyfunction


    def run(self):
        logger.debug("Start MTPDataCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logger.debug("_proc_message cost %f" % (time.time()-t1))
                    
#        except Exception as e:
#            logger.debug("%s except raised : %s " % (e.__class__, e.args))



    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
#        logger.debug('_proc_message')
        #解body
        msgdict = dict()
        try:
            # logger.debug(recvbuf)
            msgdict = json.loads(recvbuf)
        except:
            logger.error("parse body error")
            return
        #检查消息必选项
        if len(msgdict) == 0:
            logger.error("body lenght is zero")
            return
        if "from" not in msgdict:
            logger.error("no route in body")
            return
        msgfrom = msgdict['from']

        seqid = '0'
        if "seqid" in msgdict:
            seqid = msgdict['seqid']

        sockid = ''
        if 'sockid' in msgdict:
            sockid = msgdict['sockid']

        if "action_cmd" not in msgdict:
            logger.error("no action_cmd in msg")
            self._sendMessage(msgfrom, '{"from":%s,"error_code":"40000","seq_id":%s,"body":{},"sockid":%s)' % (self.recv_queue_name, seqid, sockid))
            return
        #构建回应消息结构
        action_cmd = msgdict['action_cmd']

        message_resp_dict = dict()
        message_resp_dict['from'] = self.recv_queue_name
        message_resp_dict['seq_id'] = seqid
        message_resp_dict['sockid'] = sockid
        message_resp_body = dict()
        message_resp_dict['body'] = message_resp_body
        
        self._proc_action(msgdict, message_resp_dict, message_resp_body)
        logger.debug("::::::RetBuf--->errorcode = %s " % (message_resp_dict.get('error_code')))

        msg_resp = json.dumps(message_resp_dict)
        logger.debug(msg_resp)
        self._sendMessage(msgfrom, msg_resp)   

    def _proc_action(self, msg_in, msg_out_head, msg_out_body):
        '''action处理入口函数'''
#        logger.debug("_proc_action action=%s" % (action))
        if 'action_cmd' not in msg_in or 'version' not in msg_in:
            logger.error("mandotry param error in action")
            msg_out_head['error_code'] = '40002'
            return
        action_cmd = msg_in['action_cmd']
#        logger.debug('action_cmd : %s' % (action_cmd))
        action_version = msg_in['version']
#        logger.debug('action_version : %s' % (action_version))
        if 'body' in msg_in:
            action_body = msg_in['body']
#            logger.debug('action_body : %s' % (action_body))
        else:
            action_body = None
            logger.debug('no action_body')

        if action_cmd == 'device_upmsg':
            self._proc_action_device_upmsg(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'push_devicemessage':
            self._proc_action_push_devicemessage(action_version, action_body, msg_out_head, msg_out_body)            
        elif action_cmd == 'test_gps':
            self._proc_action_test_gps(action_version, action_body, msg_out_head, msg_out_body)            
        elif action_cmd == 'test_health':
            self._proc_action_test_health(action_version, action_body, msg_out_head, msg_out_body)            
        else:
            msg_out_head['error_code'] = '40000'

        return

    def encrypt(self,buf):
        cipher = AES.new(self.WATCH_AES_KEY,AES.MODE_CBC,self.WATCH_AES_IV)
        returnval = base64.b64encode(cipher.encrypt(pad(buf)))
        logger.debug("encrypt %r -> %r"%(buf,returnval))
        return returnval

    def decrypt(self,buf):
        cipher = AES.new(self.WATCH_AES_KEY,AES.MODE_CBC,self.WATCH_AES_IV)
        returnval = unpad(cipher.decrypt(base64.b64decode(buf)))
        logger.debug("decrypt %r -> %r"%(buf,returnval))
        return returnval

    def makeHeader(self,cmdvalue,seq_id,version,securit,imeistr,devicetype,bodylen):
        s = struct.Struct('>HHIHBB%dsB'%(len(imeistr)))
        msglen = bodylen+len(imeistr)+13 + 24
        returnval=s.pack(*(msglen,cmdvalue,seq_id,version,securit,len(imeistr),imeistr,devicetype))
        logger.debug("makeHeader ->%r"%(returnval))
        return returnval

    def makeMPart(self,structstr,valuetuple):
        s = struct.Struct(structstr)
        returnval = s.pack(*valuetuple)
        logger.debug("makeMPart %r:%r -> %r"%(structstr,valuetuple, returnval))
        return returnval

    def makeEventTLV(self,tag,lenvalue,value):
        returnval=''
        s = struct.Struct('>BH%ds'% (len(value)))
        returnval= s.pack(*(tag,lenvalue,value))
        logger.debug("makeEventTLV T=%X,L=%X,V=%r ->%r"%(tag,lenvalue,value,returnval))
        return returnval

    def makeTLV(self,tag,lenvalue,value):
        returnval=''
        if tag == self.WATCH_TAG_STRING:
            s = struct.Struct('>HH%ds'% (len(value)))
            returnval= s.pack(*(tag,lenvalue,value))
        elif tag == self.WATCH_TAG_DATETIME:
            s = struct.Struct('>HHI')
            returnval= s.pack(*(tag,4,value))
        elif tag == self.WATCH_TAG_INT:
            s = struct.Struct('>HHI')
            returnval= s.pack(*(tag,4,value))
        elif tag == self.WATCH_TAG_SHORT:
            s = struct.Struct('>HHH')
            returnval= s.pack(*(tag,2,value))
        elif tag == self.WATCH_TAG_BYTE:
            s = struct.Struct('>HHB')
            returnval= s.pack(*(tag,1,value))
        elif tag == self.WATCH_TAG_EVENT:
            s = struct.Struct('>HH%ds'% (len(value)))
            returnval= s.pack(*(tag,lenvalue,value))
        elif tag == self.WATCH_TAG_AES:
            s = struct.Struct('>HH%ds'% (len(value)))
            returnval= s.pack(*(tag,lenvalue,value))
        elif tag == self.WATCH_TAG_SUM:
            s = struct.Struct('>HH%ds'% (len(value)))
            returnval= s.pack(*(tag,lenvalue,value))
        logger.debug("makeTLV T=%X,L=%X,V=%r ->%r"%(tag,lenvalue,value,returnval))
        return returnval

    def parseTlV(self,bufarray):
        returnlist = list()
        tmpbufarray = bufarray
        while len(tmpbufarray) > 0:
            tag = self.getValuebyStruct('>H',tmpbufarray[0:2].tostring(),2)
            if tag not in [self.WATCH_TAG_STRING,self.WATCH_TAG_DATETIME,self.WATCH_TAG_INT,self.WATCH_TAG_SHORT,self.WATCH_TAG_BYTE,self.WATCH_TAG_EVENT]:
                tmpbufarray=tmpbufarray[2:]
                continue
            tlen = self.getValuebyStruct('>H',tmpbufarray[2:4].tostring(),2)
            if tag == self.WATCH_TAG_STRING:
                # s = struct.Struct('%ds'% (tlen))
                # returnval= s.unpack(tmpbufarray[4:4+tlen].tostring())[0]
                returnval = self.getValuebyStruct('%ds'% (tlen), tmpbufarray[4:4+tlen].tostring(),tlen)
                returnlist.append(returnval)
            elif tag == self.WATCH_TAG_DATETIME:
                # s = struct.Struct('>I')
                # returnval= s.unpack(tmpbufarray[4:4+tlen].tostring())[0]
                returnval = self.getValuebyStruct('>I', tmpbufarray[4:4+tlen].tostring(),tlen)
                returnlist.append(returnval)
            elif tag == self.WATCH_TAG_INT:
                # s = struct.Struct('>I')
                # returnval= s.unpack(tmpbufarray[4:4+tlen].tostring())[0]
                returnval = self.getValuebyStruct('>I', tmpbufarray[4:4+tlen].tostring(),tlen)
                returnlist.append(returnval)
            elif tag == self.WATCH_TAG_SHORT:
                # s = struct.Struct('>H')
                # returnval= s.unpack(tmpbufarray[4:4+tlen].tostring())[0]
                returnval = self.getValuebyStruct('>H', tmpbufarray[4:4+tlen].tostring(),tlen)
                returnlist.append(returnval)
            elif tag == self.WATCH_TAG_BYTE:
                # s = struct.Struct('B')
                # returnval= s.unpack(tmpbufarray[4:4+tlen].tostring())[0]
                returnval = self.getValuebyStruct('%dB'%(tlen), tmpbufarray[4:4+tlen].tostring(),tlen)
                returnlist.append(returnval)
            elif tag == self.WATCH_TAG_EVENT:
                # s = struct.Struct('%ds'% (tlen))
                # returnval= s.unpack(tmpbufarray[4:4+tlen].tostring())[0]
                returnval = self.getValuebyStruct('%ds'% (tlen), tmpbufarray[4:4+tlen].tostring(),tlen)
                returnlist.append(returnval)

            tmpbufarray = tmpbufarray[4+tlen:]
        logger.debug("parseTlV %s ->%r"%(bufarray.tostring(),returnlist))
        return returnlist
    def parseAESPart(self,recvedbuflist):
        logger.debug("parseAESPart")
        paramlist = list()
        if len(recvedbuflist):
            recvbufarray = array.array('B',)
            recvbufarray.fromlist(recvedbuflist)
            tag = self.getValuebyStruct('>H',recvbufarray[0:2].tostring(),2)
            if tag == self.WATCH_TAG_AES:
                aeslen = self.getValuebyStruct('>H',recvbufarray[2:4].tostring(),2)
                if len(recvbufarray) > aeslen:
                    aesbuf = recvbufarray[4:aeslen+4].tostring()
                    decryptbuf = self.decrypt(aesbuf)
                    paramarray = array.array('B',)
                    paramarray.fromstring(decryptbuf)
                    paramlist = self.parseTlV(paramarray)
        return paramlist
    def _proc_action_test_gps(self, version, action_body, retdict, retbody):
        logger.debug("_proc_action_test_gps %r"%(action_body))
        imei = action_body.get('imei')
        survkey = self.KEY_DEVICESUVKEY % (imei)
        if self._redis.exists(survkey) is False:
            logger.error("no need SURVEILLANCE")
            return
        deviceinfokey = self.KEY_DEVICEINFO % (imei)
        imeikey = deviceinfokey
        if self._redis.exists(deviceinfokey) is False:        
            imeiinfo = self.collect_gearinfo.find_one({'imei':int(imei)})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                return
            else:
                for key in imeiinfo:
                    if key in ['_id']:
                        imeiinfo[key] = imeiinfo[key].__str__()
                    elif isinstance(imeiinfo[key],datetime.datetime):
                        imeiinfo[key] = imeiinfo[key].__str__()

                self._redis.hmset(deviceinfokey, imeiinfo)
                logger.error("get back from db imei:%s"%(deviceinfokey))

        imeiinfo = self._redis.hgetall(imeikey)
        memberid = imeiinfo.get('follow')
        if memberid is None or memberid == '':
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return            
        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
        if memberinfo is None:
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return
        if memberinfo['state'] != self.MEMBER_STATE_SURVEILLANCE:
            logger.error("Member [%s] is not in survellance mode!!!",memberid)
            return

        locationtimeout = memberinfo.get('location_timeout')
        if locationtimeout is None:
            locationtimeout = self.DEFAULT_LOCATION_TIMEOUT

        insertdict = dict()
        time_device = int(time.time())
        timet = str(time_device)
        timestamp = datetime.datetime.now()
        healthid = None
        location_type = self.LOCATION_TYPE_GPS
        longitude = action_body.get("lng")
        latitude = action_body.get("lat")
        vt = 0
        angle = 0
        battery_level = 70
        altitude = 0
        # healthid = gpsstrlist[14][6:]
        #写数据
        insertdict['satellite'] = 3
        insertdict['imei'] = int(imei)
        insertdict['timet'] = timet
        insertdict['time_device'] = time_device
        insertdict['battery_level'] = battery_level
        insertdict['longitude'] = longitude
        insertdict['latitude'] = latitude
        insertdict['altitude'] = altitude
        insertdict['vt'] = vt
        insertdict['location_type'] = location_type
        insertdict['timestamp'] = timestamp
        insertdict['angle'] = angle
        insertdict['username'] = memberinfo['username']
        insertdict['memberid'] = memberinfo['_id'].__str__()
        insertdict['nation'] = memberinfo.get('nation')
        insertdict['nickname'] = memberinfo.get('nickname')
        insertdict['codetype'] = memberinfo.get('codetype')
        insertdict['passdate'] = memberinfo.get('passdate')
        insertdict['passland'] = memberinfo.get('passland')
        insertdict['devicetype'] = memberinfo.get('devicetype')
        insertdict['deviceid'] = memberinfo.get('deviceid')
        logger.debug(insertdict)
        obj = self.collect_gpsinfo.insert_one(insertdict)
        insertobj = obj.inserted_id
        #更新到gearinfo中去
        if insertobj:
            updatedict = dict()
            updatedict['battery_level'] = battery_level
            updatedict['last_long'] = longitude
            updatedict['last_lat'] = latitude
            updatedict['last_at'] = altitude
            updatedict['last_vt'] = vt
            updatedict['last_angle'] = angle
            updatedict['last_location_objectid'] = insertobj.__str__()
            updatedict['last_location_type'] = self.LOCATION_TYPE_GPS
            updatedict['last_location_time'] = timestamp.__str__()

            self._redis.hmset(imeikey, updatedict)
            updatedict['last_location_time'] = timestamp
            self.collect_gearinfo.update_one({'imei':int(imei)},{'$set':updatedict})
            self.collect_memberinfo.update_one({'_id':memberinfo['_id']},{'$set':updatedict})
        #查看是否需要围栏服务
        if 'alarm_enable' in memberinfo and memberinfo['alarm_enable'] == '1':
            body = dict()
            body['imei'] = int(imei)
            body['latitude'] = latitude
            body['longitude'] = longitude
            body['imeikey'] = imeikey
            body['memberid'] = memberinfo['_id'].__str__()
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'check_location'
            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
            action['from'] = ''
            if 'session' in self._config:
                self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))
        #取逆向地理信息
            body = dict()
            body['lat'] = latitude
            body['lng'] = longitude
            body['gps_objectid'] = insertobj.__str__()
            body['memberid'] = memberinfo['_id'].__str__()
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'geo_location'
            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
            action['from'] = ''
            if 'session' in self._config:
                self._sendMessage(self._config['lbs_proxy']['Consumer_Queue_Name'], json.dumps(action))
        #设置用户定位信息标记
        memberlocationkey  = self.KEY_MEMBER_LOCSTATE % (memberid)
        if self._redis.exists(memberlocationkey) is False:
            #说明当前的用户定位已经超时报警了，现在要消除此类报警
            body = dict()
            body['memberid'] = memberid
            body['type'] = self.ALARM_TYPE_TIME
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'alarm_close'
            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
            action['from'] = ''
            self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))

        self._redis.set(memberlocationkey,'1')
        self._redis.expire(memberlocationkey,locationtimeout)

    def parseGPSinfo(self,imei,gpsstring):
        logger.debug("parseGPSinfo %s"%(gpsstring))
        survkey = self.KEY_DEVICESUVKEY % (imei)
        if self._redis.exists(survkey) is False:
            logger.error("no need SURVEILLANCE")
            return
        deviceinfokey = self.KEY_DEVICEINFO % (imei)
        imeikey = deviceinfokey
        if self._redis.exists(deviceinfokey) is False:        
            imeiinfo = self.collect_gearinfo.find_one({'imei':int(imei)})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                return
            else:
                for key in imeiinfo:
                    if key in ['_id']:
                        imeiinfo[key] = imeiinfo[key].__str__()
                    elif isinstance(imeiinfo[key],datetime.datetime):
                        imeiinfo[key] = imeiinfo[key].__str__()

                self._redis.hmset(deviceinfokey, imeiinfo)
                logger.error("get back from db imei:%s"%(deviceinfokey))

        imeiinfo = self._redis.hgetall(imeikey)
        memberid = imeiinfo.get('follow')
        if memberid is None or memberid == '':
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return            
        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
        if memberinfo is None:
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return
        if memberinfo['state'] != self.MEMBER_STATE_SURVEILLANCE:
            logger.error("Member [%s] is not in survellance mode!!!",memberid)
            return

        locationtimeout = memberinfo.get('location_timeout')
        if locationtimeout is None:
            locationtimeout = self.DEFAULT_LOCATION_TIMEOUT

        gpsstrlist = gpsstring.split(';')
        insertdict = dict()
        if len(gpsstrlist) < 14:
            logger.error('parseGPSinfo: error gpslist lenght')
            return
        time_device = int(time.time())
        timet = str(time_device)
        timestamp = datetime.datetime.now()
        healthid = None
        if gpsstrlist[0] == 'A':
            logger.debug("Has GPS")
            location_type = self.LOCATION_TYPE_GPS
            latstr = gpsstrlist[1]
            lngstr = gpsstrlist[2]
            longitude = 0
            latitude = 0
            vt = 0
            angle = 0
            if latstr.startswith('N'):
                latitude = float(latstr[1:])
            else:
                latitude = -float(latstr[1:])
            if lngstr.startswith('E'):
                longitude = float(lngstr[1:])
            else:
                longitude = -float(lngstr[1:])
            vt = float(gpsstrlist[3][2:])
            angle = float(gpsstrlist[4][2:])
            battery_level = int(gpsstrlist[8][3:])
            # healthid = gpsstrlist[14][6:]
            #写数据
            insertdict['satellite'] = int(gpsstrlist[5][3:])
            insertdict['imei'] = int(imei)
            insertdict['timet'] = timet
            insertdict['time_device'] = time_device
            insertdict['battery_level'] = battery_level
            insertdict['longitude'] = longitude
            insertdict['latitude'] = latitude
            insertdict['altitude'] = altitude
            insertdict['vt'] = vt
            insertdict['location_type'] = location_type
            insertdict['timestamp'] = timestamp
            insertdict['angle'] = angle
            insertdict['username'] = memberinfo['username']
            insertdict['memberid'] = memberinfo['_id'].__str__()
            insertdict['nation'] = memberinfo.get('nation')
            insertdict['nickname'] = memberinfo.get('nickname')
            insertdict['codetype'] = memberinfo.get('codetype')
            insertdict['passdate'] = memberinfo.get('passdate')
            insertdict['passland'] = memberinfo.get('passland')
            insertdict['devicetype'] = memberinfo.get('devicetype')
            insertdict['deviceid'] = memberinfo.get('deviceid')
            logger.debug(insertdict)
            obj = self.collect_gpsinfo.insert_one(insertdict)
            insertobj = obj.inserted_id
            #更新到gearinfo中去
            if insertobj:
                updatedict = dict()
                updatedict['battery_level'] = battery_level
                updatedict['last_long'] = longitude
                updatedict['last_lat'] = latitude
                updatedict['last_at'] = altitude
                updatedict['last_vt'] = vt
                updatedict['last_angle'] = angle
                updatedict['last_location_objectid'] = insertobj.__str__()
                updatedict['last_location_type'] = self.LOCATION_TYPE_GPS
                updatedict['last_location_time'] = timestamp.__str__()

                self._redis.hmset(imeikey, updatedict)
                updatedict['last_location_time'] = timestamp
                self.collect_gearinfo.update_one({'imei':int(imei)},{'$set':updatedict})
                self.collect_memberinfo.update_one({'_id':memberinfo['_id']},{'$set':updatedict})
            #查看是否需要围栏服务
            if 'alarm_enable' in memberinfo and memberinfo['alarm_enable'] == '1':
                body = dict()
                body['imei'] = int(imei)
                body['latitude'] = latitude
                body['longitude'] = longitude
                body['imeikey'] = imeikey
                body['memberid'] = memberinfo['_id'].__str__()
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'check_location'
                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                action['from'] = ''
                if 'session' in self._config:
                    self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))
            #取逆向地理信息
                body = dict()
                body['lat'] = latitude
                body['lng'] = longitude
                body['gps_objectid'] = insertobj.__str__()
                body['memberid'] = memberinfo['_id'].__str__()
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'geo_location'
                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                action['from'] = ''
                if 'session' in self._config:
                    self._sendMessage(self._config['lbs_proxy']['Consumer_Queue_Name'], json.dumps(action))
        else:
            logger.debug("Has LBS")
            location_type = self.LOCATION_TYPE_LBS
            battery_level = int(gpsstrlist[8][3:])
            gsmsignal = gpsstrlist[7][3:]
            lbsstr = gpsstrlist[6][3:]
            wifistr = gpsstrlist[10][4:]
            rssistr = gpsstrlist[11][4:]
            # healthid = gpsstrlist[14][6:]

            lbslist = list()
            wifilist = list()
            tmplbssplitlist = lbsstr.split('#')
            for i in xrange(0,len(tmplbssplitlist)):
                l = tmplbssplitlist[i]
                if len(l):
                    tmplbslist = l.split(',')
                    if len(tmplbslist) >= 4:
                        lbsinfo = dict()
                        lbsinfo['mcc'] = tmplbslist[0]
                        lbsinfo['mnc'] = tmplbslist[1]
                        lbsinfo['lac'] = tmplbslist[2]
                        lbsinfo['cellid'] = tmplbslist[3]
                        if i == 0:
                            lbsinfo['gsm_signal'] = gsmsignal
                        else:
                            lbsinfo['gsm_signal'] = str(random.SystemRandom().randint(int(gsmsignal),int(gsmsignal)+20))
                        lbslist.append(lbsinfo)
            tmpwifisplitlist = wifistr.split('#')
            tmprssisplitlist = rssistr.split('#')
            if len(wifistr) == 0:
                wifilist.append({'wifimac':'0','wifisignal':'0'})
                wifilist.append({'wifimac':'0','wifisignal':'0'})
            else:
                for i in xrange(0,len(tmpwifisplitlist)):
                    wifiinfo = dict()
                    wifiinfo['wifimac'] = tmpwifisplitlist[i]
                    wifiinfo['wifisignal'] = tmprssisplitlist[i]
                    wifilist.append(wifiinfo)

            insertdict['lbs'] = lbslist
            insertdict['timet'] = timet
            insertdict['time_device'] = time_device
            insertdict['location_type'] = location_type
            insertdict['timestamp'] = timestamp
            insertdict['battery_level'] = battery_level
            insertdict['wifi'] = wifilist
            insertdict['imei'] = int(imei)
            insertdict['memberid'] = memberinfo['_id'].__str__()
            insertdict['username'] = memberinfo.get('username')
            insertdict['nation'] = memberinfo.get('nation')
            insertdict['nickname'] = memberinfo.get('nickname')
            insertdict['codetype'] = memberinfo.get('codetype')
            insertdict['passdate'] = memberinfo.get('passdate')
            insertdict['passland'] = memberinfo.get('passland')
            insertdict['devicetype'] = memberinfo.get('devicetype')
            insertdict['deviceid'] = memberinfo.get('deviceid')
            insertdict['alarm_enable'] = memberinfo.get('alarm_enable')
            logger.debug(insertdict)
            obj = self.collect_lbsinfo.insert_one(insertdict)
            insertobj = obj.inserted_id
            #更新到gearinfo中去
            if insertobj:
                updatedict = dict()
                updatedict['battery_level'] = battery_level
                updatedict['last_lbs_objectid'] = insertobj.__str__()
                self._redis.hmset(imeikey, updatedict)
                self.collect_gearinfo.update_one({'imei':int(imei)},{'$set':updatedict})
            #发送基站wifi定位解析的请求：
                body = dict()
                body['imei'] = int(imei)
                body['imeikey'] = imeikey
                body['last_lbs_objectid'] = imeiinfo.get('last_lbs_objectid')
                body['lbs_objectid'] = insertobj.__str__()
                body['lbs'] = lbslist
                body['wifi'] = wifilist

                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'lbs_location'
                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                action['from'] = ''
                if 'lbs_proxy' in self._config:
                    self._sendMessage(self._config['lbs_proxy']['Consumer_Queue_Name'], json.dumps(action))
        #设置用户定位信息标记
        memberlocationkey  = self.KEY_MEMBER_LOCSTATE % (memberid)
        if self._redis.exists(memberlocationkey) is False:
            #说明当前的用户定位已经超时报警了，现在要消除此类报警
            body = dict()
            body['memberid'] = memberid
            body['type'] = self.ALARM_TYPE_TIME
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'alarm_close'
            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
            action['from'] = ''
            self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))

        self._redis.set(memberlocationkey,'1')
        self._redis.expire(memberlocationkey,locationtimeout)

        # self.set_online_flag(imei)

    def set_online_flag(self, imei):
        logger.debug("set_online_flag [%s] "% (imei))
        setkey = self.KEY_IMEI_ONLINE_FLAG % (imei)
        sessionkey = self.KEY_IMEI_SESSION_TIMEOUT % (imei)
        if self._redis.exists(sessionkey) is False:
            #如果sessionkey不存在，就去检查当前要做的业务，主要是触发几个通知接口，结束监管的和健康上报的
            body = dict()
            body['imei'] = imei
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'check_notify_logic'
            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
            action['from'] = ''
            if 'session' in self._config:
                self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))
            self._redis.set(sessionkey,imei)
            self._redis.expire(sessionkey,self.DEFAULT_SESSION_CHECK_TIMEOUT)

        # if self._redis.exists(setkey) is False:
        #     #发现是重新登陆时，交给session模块去处理未发送的消息
        #     body = dict()
        #     body['imei'] = imei
        #     action = dict()
        #     action['body'] = body
        #     action['version'] = '1.0'
        #     action['action_cmd'] = 'check_undo_msg'
        #     action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
        #     action['from'] = ''
        #     if 'session' in self._config:
        #         self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action)) 您已经24小时未申报健康状态，请及时申报

        self._redis.set(setkey, datetime.datetime.now().__str__())
        self._redis.expire(setkey, self.GEAR_ONLINE_TIMEOUT)

    def parseStepInfo(self,imei,step):
        logger.debug("parseStepInfo [%s]->[%d]"%(imei,step))
        deviceinfokey = self.KEY_DEVICEINFO % (imei)
        imeikey = deviceinfokey
        imeiinfo = self._redis.hgetall(imeikey)
        if imeiinfo is None:
            logger.error("No imei cache")
            return
        memberid = imeiinfo.get('follow')
        if memberid is None or memberid == '':
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return            
        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
        if memberinfo is None:
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return
        datestr = time.strftime('%Y-%m-%d',time.localtime(time.time()))
        insertdata = dict()
        insertdata['member_id'] = memberid
        insertdata['username'] = memberinfo.get("username")
        insertdata['date_str'] = datestr
        insertdata['data_type'] = 'step'
        insertdata['value'] = step
        insertdata['value2'] = 0
        insertdata['imei'] = int(imei)
        insertdata['deviceid'] = imei
        insertdata['datetime'] = datetime.datetime.now()
        insertdata['timestamp'] = int(time.time())
        self.collect_bodyfunction.insert_one(insertdata)

    def parseHeartInfo(self,imei,heart):
        logger.debug("parseHeartInfo [%s]->[%d]"%(imei,heart))
        deviceinfokey = self.KEY_DEVICEINFO % (imei)
        imeikey = deviceinfokey
        imeiinfo = self._redis.hgetall(imeikey)
        if imeiinfo is None:
            logger.error("No imei cache")
            return
        memberid = imeiinfo.get('follow')
        if memberid is None or memberid == '':
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return            
        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
        if memberinfo is None:
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return
        datestr = time.strftime('%Y-%m-%d',time.localtime(time.time()))
        insertdata = dict()
        insertdata['member_id'] = memberid
        insertdata['date_str'] = datestr
        insertdata['data_type'] = 'heart'
        insertdata['value'] = heart
        insertdata['value2'] = 0
        insertdata['imei'] = int(imei)
        insertdata['deviceid'] = imei
        insertdata['datetime'] = datetime.datetime.now()
        insertdata['timestamp'] = int(time.time())
        self.collect_bodyfunction.insert_one(insertdata)

    def parseBloodPressureInfo(self,imei,sbp,dbp):
        logger.debug("parseBloodPressureInfo [%s]->[%d/%d]"%(imei,sbp,dbp))
        deviceinfokey = self.KEY_DEVICEINFO % (imei)
        imeikey = deviceinfokey
        imeiinfo = self._redis.hgetall(imeikey)
        if imeiinfo is None:
            logger.error("No imei cache")
            return
        memberid = imeiinfo.get('follow')
        if memberid is None or memberid == '':
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return            
        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
        if memberinfo is None:
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return
        datestr = time.strftime('%Y-%m-%d',time.localtime(time.time()))
        insertdata = dict()
        insertdata['member_id'] = memberid
        insertdata['date_str'] = datestr
        insertdata['data_type'] = 'bloodpress'
        insertdata['value'] = sbp
        insertdata['value2'] = dbp
        insertdata['imei'] = int(imei)
        insertdata['deviceid'] = imei
        insertdata['datetime'] = datetime.datetime.now()
        insertdata['timestamp'] = int(time.time())
        self.collect_bodyfunction.insert_one(insertdata)

    def _proc_action_test_health(self, version, action_body, retdict, retbody):
        logger.debug("_proc_action_test_health : [%r]" % (action_body))
        imei = action_body.get("imei")
        healthid = action_body.get("healthid")
        if imei is '' or healthid == '':
            logger.error("cmdbody error")
            return
        imei_str = imei
        imei_int = int(imei_str)
        info_id = healthid
        # self.set_online_flag(imei_str)
        survkey = self.KEY_DEVICESUVKEY % (imei_str)
        if self._redis.exists(survkey) is False:
            logger.error("no need SURVEILLANCE")
            return
        deviceinfokey = self.KEY_DEVICEINFO % (imei_str)
        imeikey = deviceinfokey
        if self._redis.exists(deviceinfokey) is False:        
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei_int})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                return
            else:
                for key in imeiinfo:
                    if key in ['_id']:
                        imeiinfo[key] = imeiinfo[key].__str__()
                    elif isinstance(imeiinfo[key],datetime.datetime):
                        imeiinfo[key] = imeiinfo[key].__str__()

                self._redis.hmset(deviceinfokey, imeiinfo)
                logger.error("get back from db imei:%s"%(deviceinfokey))

        imeiinfo = self._redis.hgetall(imeikey)
        memberid = imeiinfo.get('follow')
        if memberid is None or memberid == '':
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return            
        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
        if memberinfo is None:
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return
        if memberinfo['state'] != self.MEMBER_STATE_SURVEILLANCE:
            logger.error("Member [%s] is not in survellance mode!!!",memberid)
            return

        declareinfo = self.collect_infotemp.find_one({'info_id':info_id})
        if declareinfo is None:
            logger.error("ERRORCODE_NO_DECLARE_INFO")
            return
        declarestate = declareinfo.get('state')
        if declarestate is None or declarestate != self.DECLARE_STATE_ACTIVE:
            logger.error("ERRORCODE_DECLARE_NOT_ACTIVE")
            return

        declarealarm = declareinfo.get('alarm')
        if declarealarm is None:
            declarealarm = 0

        #设置用户定位信息标记
        memberhealthkey  = self.KEY_MEMBER_HEALTHSTATE % (memberid)
        if self._redis.exists(memberhealthkey) is False:
            #说明当前的用户状态已经超时报警了，现在要消除此类报警
            body = dict()
            body['memberid'] = memberid
            body['type'] = self.ALARM_TYPE_HEALTHTIMEOUT
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'alarm_close'
            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
            action['from'] = ''
            self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))
            
        self._redis.set(memberhealthkey,'1')
        self._redis.expire(memberhealthkey,self.DEFAULT_HEALTH_TIMEOUT)


        stateinfo = self.collect_infostate.find_one({'memberid':memberid})
        needkey = ['codetype','username','simnumber','nation','devicetype','passland','deviceid','nickname','illgroup','nickname']
        memberinsertdict = dict()
        for key in needkey:
            memberinsertdict[key] = memberinfo.get(key)
            
        insertdict = dict()
        insertdict['username'] = memberinfo['username']
        insertdict['memberid'] = memberid
        insertdict['info_id'] = info_id
        insertdict['info_desc'] = declareinfo.get('info_desc')
        insertdict['timestamp'] = datetime.datetime.now()
        insertdict.update(memberinsertdict)

        if stateinfo is None:
            self.collect_infostate.insert_one(insertdict)
        else:
            self.collect_infostate.update_one({'memberid':memberid},{'$set':{'info_id':info_id,'info_desc':declareinfo.get('info_desc'),'timestamp':datetime.datetime.now()}})

        self.collect_infolog.insert_one(insertdict)

        updatedict = dict()
        updatedict['last_health'] = info_id
        updatedict['last_health_time'] = datetime.datetime.now()

        self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':updatedict})

        #状态报警
        if declarealarm == 1:
            # memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is not None:
                body = dict()
                body['username'] = memberinfo['username']
                body['memberid'] = memberid
                body['type'] = self.ALARM_TYPE_HEALTH
                body['value'] = info_id
                body['latitude'] = memberinfo.get('last_long')
                body['longitude'] = memberinfo.get('last_lat')
                if 'last_location_time' in memberinfo:
                    body['last_location_time'] = memberinfo['last_location_time'].strftime('%Y-%m-%d %H:%M:%S')
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'alarm_create'
                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                action['from'] = ''
                self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))
        else:
            if memberinfo is not None:
                body = dict()
                body['memberid'] = memberid
                body['type'] = self.ALARM_TYPE_HEALTH
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'alarm_close'
                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                action['from'] = ''
                self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))        

    def parseHealthDeclare(self,imei,healthid):
        logger.debug("parseHealthDeclare : [%s]->%s" % (imei,healthid))
        if imei is '' or healthid == '':
            logger.error("cmdbody error")
            return
        imei_str = imei
        imei_int = int(imei_str)
        info_id = healthid
        # self.set_online_flag(imei_str)
        survkey = self.KEY_DEVICESUVKEY % (imei_str)
        if self._redis.exists(survkey) is False:
            logger.error("no need SURVEILLANCE")
            return
        deviceinfokey = self.KEY_DEVICEINFO % (imei_str)
        imeikey = deviceinfokey
        if self._redis.exists(deviceinfokey) is False:        
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei_int})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                return
            else:
                for key in imeiinfo:
                    if key in ['_id']:
                        imeiinfo[key] = imeiinfo[key].__str__()
                    elif isinstance(imeiinfo[key],datetime.datetime):
                        imeiinfo[key] = imeiinfo[key].__str__()

                self._redis.hmset(deviceinfokey, imeiinfo)
                logger.error("get back from db imei:%s"%(deviceinfokey))

        imeiinfo = self._redis.hgetall(imeikey)
        memberid = imeiinfo.get('follow')
        if memberid is None or memberid == '':
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return            
        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
        if memberinfo is None:
            logger.error("Member [%s] is not EXIST!!!",memberid)
            return
        if memberinfo['state'] != self.MEMBER_STATE_SURVEILLANCE:
            logger.error("Member [%s] is not in survellance mode!!!",memberid)
            return

        declareinfo = self.collect_infotemp.find_one({'info_id':info_id})
        if declareinfo is None:
            logger.error("ERRORCODE_NO_DECLARE_INFO")
            return
        declarestate = declareinfo.get('state')
        if declarestate is None or declarestate != self.DECLARE_STATE_ACTIVE:
            logger.error("ERRORCODE_DECLARE_NOT_ACTIVE")
            return

        declarealarm = declareinfo.get('alarm')
        if declarealarm is None:
            declarealarm = 0

        #设置用户定位信息标记
        memberhealthkey  = self.KEY_MEMBER_HEALTHSTATE % (memberid)
        if self._redis.exists(memberhealthkey) is False:
            #说明当前的用户状态已经超时报警了，现在要消除此类报警
            body = dict()
            body['memberid'] = memberid
            body['type'] = self.ALARM_TYPE_HEALTHTIMEOUT
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'alarm_close'
            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
            action['from'] = ''
            self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))
            
        self._redis.set(memberhealthkey,'1')
        self._redis.expire(memberhealthkey,self.DEFAULT_HEALTH_TIMEOUT)


        stateinfo = self.collect_infostate.find_one({'memberid':memberid})
        needkey = ['codetype','username','simnumber','nation','devicetype','passland','deviceid','nickname','illgroup','nickname']
        memberinsertdict = dict()
        for key in needkey:
            memberinsertdict[key] = memberinfo.get(key)
            
        insertdict = dict()
        insertdict['username'] = memberinfo['username']
        insertdict['memberid'] = memberid
        insertdict['info_id'] = info_id
        insertdict['info_desc'] = declareinfo.get('info_desc')
        insertdict['timestamp'] = datetime.datetime.now()
        insertdict.update(memberinsertdict)

        if stateinfo is None:
            self.collect_infostate.insert_one(insertdict)
        else:
            self.collect_infostate.update_one({'memberid':memberid},{'$set':{'info_id':info_id,'info_desc':declareinfo.get('info_desc'),'timestamp':datetime.datetime.now()}})

        self.collect_infolog.insert_one(insertdict)

        updatedict = dict()
        updatedict['last_health'] = info_id
        updatedict['last_health_time'] = datetime.datetime.now()

        self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':updatedict})

        #状态报警
        if declarealarm == 1:
            # memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is not None:
                body = dict()
                body['username'] = memberinfo['username']
                body['memberid'] = memberid
                body['type'] = self.ALARM_TYPE_HEALTH
                body['value'] = info_id
                body['latitude'] = memberinfo.get('last_long')
                body['longitude'] = memberinfo.get('last_lat')
                if 'last_location_time' in memberinfo:
                    body['last_location_time'] = memberinfo['last_location_time'].strftime('%Y-%m-%d %H:%M:%S')
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'alarm_create'
                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                action['from'] = ''
                self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))
        else:
            if memberinfo is not None:
                body = dict()
                body['memberid'] = memberid
                body['type'] = self.ALARM_TYPE_HEALTH
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'alarm_close'
                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                action['from'] = ''
                self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))


    def parseConfigTrapParam(self,imei,subcmd,paramlist):
        logger.debug("parseConfigTrapParam")
        if subcmd == self.WATCH_SUB_CMD_UPLOAD_ALARM:
            if len(paramlist) >= 3:
                alarmtype = paramlist[0]
                timestamp = paramlist[1]
                gpsstr = paramlist[2]
                self.parseGPSinfo(imei,gpsstr)
        elif subcmd == self.WATCH_SUB_CMD_UPLOAD_LOCATION:
            if len(paramlist) >= 2:
                gpsstr = paramlist[1]
                self.parseGPSinfo(imei,gpsstr)
        elif subcmd == self.WATCH_SUB_CMD_UPLOAD_MANUAL_LOCATION:
            if len(paramlist) >= 2:
                gpsstr = paramlist[1]
                self.parseGPSinfo(imei,gpsstr)
        elif subcmd == self.WATCH_SUB_CMD_UPLOAD_STEP:
            if len(paramlist) >= 3:
                gpsstr = paramlist[1]
                step = paramlist[2]
                # self.parseGPSinfo(imei,gpsstr)
                self.parseStepInfo(imei,int(step))
        elif subcmd == self.WATCH_SUB_CMD_UPLOAD_HEART:
            if len(paramlist) >= 3:
                datatuple = paramlist[2]
                logger.debug(datatuple.__class__)
                if isinstance(datatuple,tuple):
                    heart = datatuple[0]
                    sbp = datatuple[1]
                    dbp = datatuple[2]
                    if heart > 0:
                        self.parseHeartInfo(imei,heart)
                    if sbp >0 and dbp > 0:
                        self.parseBloodPressureInfo(imei,sbp,dbp)
        elif subcmd == self.WATCH_SUB_CMD_REQ_HEALTH:
            if len(paramlist)>=1:
                lang = paramlist[0]

        elif subcmd == self.WATCH_SUB_CMD_DECLARE_HEALTH:
            if len(paramlist) >= 1:
                healthid = paramlist[0]
                self.parseHealthDeclare(imei,healthid)


    def packRequestHealthInfo(self,imei_str,lang):
        logger.debug("packRequestHealthInfo : [%s]->%s" % (imei_str,lang))
        if imei_str is '':
            logger.error("cmdbody error")
            return ''
        language = 'chs'
        if lang == 'english':
            language = 'eng'

        imei = int(imei_str)
        default_contentkey = 'info_eng'

        imeiinfo = None
        deviceinfokey = self.KEY_DEVICEINFO % (imei_str)
        imeikey = deviceinfokey
        if self._redis.exists(deviceinfokey) is False:
            imei = int(imei_str)
            imeiinfo = self.collect_gearinfo.find_one({'imei':imei})
            if imeiinfo == None:
                logger.error("can't found imei in cache!!!")
                return ''
            else:
                for key in imeiinfo:
                    if key in ['_id']:
                        imeiinfo[key] = imeiinfo[key].__str__()
                    elif isinstance(imeiinfo[key],datetime.datetime):
                        imeiinfo[key] = imeiinfo[key].__str__()

                self._redis.hmset(deviceinfokey, imeiinfo)
                logger.error("get back from db imei:%s"%(deviceinfokey))
        else:
            imeiinfo = self._redis.hgetall(deviceinfokey)
        
        # self.set_online_flag(imei_str)
        #更新手表当前的语言种类
        updateDeviceInfodict = dict()
        updateDeviceInfodict['language'] = language
        self.collect_gearinfo.update_one({'imei':imei},{"$set":updateDeviceInfodict})
        self._redis.hmset(deviceinfokey,updateDeviceInfodict)

        #查找所有的通知
        infolist = self.collect_infotemp.find({'state':self.DECLARE_STATE_ACTIVE})
        sendbuf = ""
        sendbuflist = list()
        for infotemp in infolist:
            logger.debug(infotemp)
            content = ""
            contentkey = 'info_%s' % (language)
            if contentkey in infotemp:
                content = infotemp[contentkey]
            else:
                content = infotemp.get(default_contentkey)
            
            contentbuf = "%s,%s" % (infotemp['info_id'],content)
            sendbuflist.append(contentbuf)

        sendbuf = ";".join(sendbuflist)
        if sendbuf.endswith(';') is False:
            sendbuf = '%s;'%(sendbuf)

        sendbuf = sendbuf.encode('UTF-8')

        return self.makeTLV(self.WATCH_TAG_STRING,len(sendbuf),sendbuf)

    def makeConfigParamTLVandEncrypt(self,imei,subcmd,paramlist):
        tlvbody = ''
        precrypttlvbody = ''
        if subcmd == self.WATCH_SUB_CMD_SYNC_TIME:
            precrypttlvbody = self.makeTLV(WATCH_TAG_DATETIME,4,int(time.time()))
        elif subcmd == self.WATCH_SUB_CMD_REQ_HEALTH:
            lang = ''
            if len(paramlist)>=1:
                lang = paramlist[0]
            precrypttlvbody = self.packRequestHealthInfo(imei,lang)

        if len(precrypttlvbody) > 0:
            crypttlvbody = self.encrypt(precrypttlvbody)
            tlvbody = self.makeTLV(self.WATCH_TAG_AES,len(crypttlvbody),crypttlvbody)
        
        logger.debug("makeConfigParamTLVandEncrypt[%X]->%r" % (subcmd, tlvbody))
        return tlvbody

    def _proc_action_device_upmsg(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'login', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                        'phone_name':O
                        'phone_os':O
                        'app_version':O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                   body:{
                       tid:O
                       activate_flag:O
                   }
                }
                    msgdict = dict()
                    msgdict['action_cmd'] = 'device_upmsg'
                    msgbody = dict()
                    msgdict['body'] = msgbody
                    msgbody['recvedbuf'] = msgarray[bodybeginlen:].tolist()
                    msgbody['imei'] = imei_str
                    msgbody['devicetype'] = devicetype
                    msgbody['seq_id'] = seq_id
                    msgbody['cmdname'] = cmdvalue
                    msgbody['msglen'] = msglen
                    msgbody['bodylen'] = msglen - bodybeginlen
                    msgbody['sockid'] = sockid
                    msgdict['sockid'] = sockid
                    msgdict['from'] = ''
                    msgdict['version'] = '1.0'

        '''
        try:
        # if 1:
            imei = action_body.get('imei')
            if isinstance(imei,unicode):
                imei = imei.encode('ascii')
            devicetype = action_body.get('devicetype')
            cmdname = action_body.get('cmdname')
            seq_id = action_body.get('seq_id')
            recvedbuflist = action_body.get('recvedbuf')
            sockid = action_body.get('sockid')
            secuirity = action_body.get('secuirity')
            version = action_body.get('version')
            logger.debug(" into _proc_action_device_upmsg[%s]->cmdname[0x%X][seqid:0x%X]"%(imei,cmdname,seq_id))
            returncmdname = 0
            returnmsglen = 0
            bodylen = 0
            msgbody = ''
            msghead = ''
            msgsum = ''
            imeiinfo = self.collect_gearinfo.find_one({"deviceid":imei})
            imeiresult = self.WATCH_RESULT_OK
            if imeiinfo is None:
                if cmdname == self.WATCH_CMD_REGISTER:
                    returncmdname = self.WATCH_CMD_REGISTER_ACK
                    imeiresult = self.WATCH_REGISTER_RESULT_ERR_NO_IMEI
                    mpart = self.makeMPart('B',tuple([imeiresult]))
                    msgbody = mpart
                    logger.debug("msgbody = %r" % (msgbody))
                    msghead = self.makeHeader(returncmdname,seq_id,version,secuirity,imei,devicetype,len(msgbody))
                    presum = msghead+msgbody+imei
                    logger.debug("presum = %r"%(presum))
                    sumvalue = hashlib.sha1(presum).digest()
                    msgsum = self.makeTLV(self.WATCH_TAG_SUM,len(sumvalue),sumvalue)
                else:
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return
            else:

                if cmdname == self.WATCH_CMD_REGISTER:
                    returncmdname = self.WATCH_CMD_REGISTER_ACK
                    mpart = self.makeMPart('B',tuple([imeiresult]))
                    tlv_uppwd = self.makeTLV(self.WATCH_TAG_STRING,len(self.WATCH_K),self.WATCH_K)
                    tlv_downpwd = self.makeTLV(self.WATCH_TAG_STRING,len(self.WATCH_K),self.WATCH_K)
                    tlv_key = self.makeTLV(self.WATCH_TAG_STRING,len(self.WATCH_AES_KEY),self.WATCH_AES_KEY)
                    msgbody = mpart+tlv_uppwd+tlv_downpwd+tlv_key
                    logger.debug("msgbody = %r" % (msgbody))
                    msghead = self.makeHeader(returncmdname,seq_id,version,secuirity,imei,devicetype,len(msgbody))
                    presum = msghead+msgbody+imei
                    logger.debug("presum = %r"%(presum))
                    sumvalue = hashlib.sha1(presum).digest()
                    msgsum = self.makeTLV(self.WATCH_TAG_SUM,len(sumvalue),sumvalue)
                elif cmdname == self.WATCH_CMD_LOGIN:
                    #先解析代码
                    paramlist = self.parseAESPart(recvedbuflist)
                    if len(paramlist) >= 2:
                        gpsstr = paramlist[0]
                        if isinstance(gpsstr,str):
                            self.parseGPSinfo(imei,gpsstr)

                    returncmdname = self.WATCH_CMD_LOGIN_ACK
                    return_timestamp =int(time.time())
                    timekey = self.KEY_IMEI_TIMESTAMP%(imei) 
                    self._redis.set(timekey,str(return_timestamp))
                    mpart = self.makeMPart('>BI',tuple([imeiresult,return_timestamp]))
                    # tlv_time = self.makeTLV(self.WATCH_TAG_DATETIME,4,int(time.time()+1))
                    # uncrypt_msgbody = mpart+tlv_time
                    msgbody = mpart
                    logger.debug("msgbody = %r" % (msgbody))
                    msghead = self.makeHeader(returncmdname,seq_id,version,secuirity,imei,devicetype,len(msgbody))
                    presum = msghead+msgbody+imei
                    logger.debug("presum = %r"%(presum))
                    sumvalue = hashlib.sha1(presum).digest()
                    msgsum = self.makeTLV(self.WATCH_TAG_SUM,len(sumvalue),sumvalue)
                elif cmdname == self.WATCH_CMD_CONFIG_TRAP:
                    #先获取子命令
                    bufarray = array.array('B',)
                    bufarray.fromlist(recvedbuflist)
                    subcmd = self.getValuebyStruct('>H',bufarray[0:2].tostring(),2)
                    logger.debug("CONFIG_TRAP subcmd = %X"%(subcmd))
                    if subcmd == self.WATCH_SUB_CMD_REQ_WEATHER:
                        logger.error("Weather no need response")
                        return
                    paramlist = self.parseAESPart(bufarray[2:].tolist())
                    self.parseConfigTrapParam(imei,subcmd,paramlist)
                    returncmdname = self.WATCH_CMD_CONFIG_TRAP_ACK

                    mpart = self.makeMPart('>HB',tuple([subcmd,imeiresult]))
                    # tlv_time = self.makeTLV(self.WATCH_TAG_DATETIME,4,int(time.time()+1))
                    # uncrypt_msgbody = mpart+tlv_time
                    tlvpart = self.makeConfigParamTLVandEncrypt(imei,subcmd,paramlist)
                    msgbody = mpart+tlvpart
                    logger.debug("msgbody = %r" % (msgbody))
                    msghead = self.makeHeader(returncmdname,seq_id,version,secuirity,imei,devicetype,len(msgbody))
                    timekey = self.KEY_IMEI_TIMESTAMP%(imei) 
                    timestring = self._redis.get(timekey)
                    if timestring is None:
                        #没有时间戳，不回应了
                        logger.error("[%s] has no timestamp Key"%(imei))
                        return
                    timepart = self.makeMPart('>I',tuple([int(timestring)]))
                    presum = msghead+msgbody+timepart+imei+self.WATCH_K
                    # logger.debug("presum = %r"%(presum))
                    sumvalue = hashlib.sha1(presum).digest()
                    msgsum = self.makeTLV(self.WATCH_TAG_SUM,len(sumvalue),sumvalue)

            
            sendbuf = msghead+msgbody+msgsum
            arraysendbuf = array.array('B',)
            arraysendbuf.fromstring(sendbuf)
            msgdict = dict()
            body = dict()
            msgdict['body'] = body
            body['sendbuf'] = arraysendbuf.tolist()
            body['imei'] = imei
            body['sockid'] = sockid
            # logger.debug(msgdict)
            if 'mtpdeviceproxy' in self._config and self._config['mtpdeviceproxy'] is not None:
                if 'Consumer_Queue_Name' in self._config['mtpdeviceproxy'] and self._config['mtpdeviceproxy']['Consumer_Queue_Name'] is not None:
                    self._redis.lpush(self._config['mtpdeviceproxy']['Consumer_Queue_Name'],json.dumps(msgdict))

            if imeiresult == self.WATCH_RESULT_OK:
                self.set_online_flag(imei)
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_push_devicemessage(self, version, action_body, retdict, retbody):
        try:
        # if 1:
            imei = action_body.get('imei')
            if isinstance(imei,unicode):
                imei = imei.encode('ascii')
            sendcontent = action_body.get('content')
            if sendcontent is None or sendcontent == '':
                logger.error("push_devicemessage error ,no content need to send (%s)" %(imei))
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if isinstance(sendcontent,unicode):
                sendcontent = sendcontent.encode('UTF-8')


            imeiinfo = self.collect_gearinfo.find_one({"deviceid":imei})
            if imeiinfo is None:
                logger.error("push_devicemessage error ,no imei(%s)" %(imei))
                retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                return
            else:
                sockkey = self.KEY_IMEI_SOCKID % (imei)
                if self._redis.exists(sockkey) is False:
                    logger.error("push_devicemessage error ,no sockkey(%s)" %(sockkey))
                    retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                    return

                devicetype = 0x10
                cmdname = self.WATCH_CMD_CONFIG_TRAP_ACK
                seq_id = random.SystemRandom().randint(1,9999)
                # sockid = action_body.get('sockid')
                secuirity = 0x1
                version = 0x5
                returncmdname = self.WATCH_CMD_CONFIG_TRAP_ACK
                # returnmsglen = 
                bodylen = 0
                msgbody = ''
                msghead = ''
                msgsum = ''                
                sockid = self._redis.get(sockkey)
                mpart = self.makeMPart('>HB',tuple([self.WATCH_SUB_CMD_EVENT,1]))
                # tlv_time = self.makeTLV(self.WATCH_TAG_DATETIME,4,int(time.time()+1))
                # uncrypt_msgbody = mpart+tlv_time
                event_tlvpart = self.makeEventTLV(self.WATCH_EVENT_NOTIFY,len(sendcontent),sendcontent)
                uncrypt_tlvpart = self.makeTLV(self.WATCH_TAG_EVENT,len(event_tlvpart),event_tlvpart)
                crypttlvbody = self.encrypt(uncrypt_tlvpart)
                tlvpart = self.makeTLV(self.WATCH_TAG_AES,len(crypttlvbody),crypttlvbody)
                msgbody = mpart+tlvpart
                logger.debug("msgbody = %r" % (msgbody))
                msghead = self.makeHeader(returncmdname,seq_id,version,secuirity,imei,devicetype,len(msgbody))
                timekey = self.KEY_IMEI_TIMESTAMP%(imei) 
                timestring = self._redis.get(timekey)
                if timestring is None:
                    #没有时间戳，不回应了
                    logger.error("[%s] has no timestamp Key"%(imei))
                    return
                timepart = self.makeMPart('>I',tuple([int(timestring)]))
                presum = msghead+msgbody+timepart+imei+self.WATCH_K
                # logger.debug("presum = %r"%(presum))
                sumvalue = hashlib.sha1(presum).digest()
                msgsum = self.makeTLV(self.WATCH_TAG_SUM,len(sumvalue),sumvalue)

                sendbuf = msghead+msgbody+msgsum
                arraysendbuf = array.array('B',)
                arraysendbuf.fromstring(sendbuf)
                msgdict = dict()
                body = dict()
                msgdict['body'] = body
                body['sendbuf'] = arraysendbuf.tolist()
                body['imei'] = imei
                body['sockid'] = sockid
                # logger.debug(msgdict)
                if 'mtpdeviceproxy' in self._config and self._config['mtpdeviceproxy'] is not None:
                    if 'Consumer_Queue_Name' in self._config['mtpdeviceproxy'] and self._config['mtpdeviceproxy']['Consumer_Queue_Name'] is not None:
                        self._redis.lpush(self._config['mtpdeviceproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
            retdict['error_code'] = self.ERRORCODE_OK

            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL        
        

if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'mtpdatacenter' in _config and _config['mtpdatacenter'] is not None:
        if 'thread_count' in _config['mtpdatacenter'] and _config['mtpdatacenter']['thread_count'] is not None:
            thread_count = int(_config['mtpdatacenter']['thread_count'])

    for i in xrange(0, thread_count):
        memberlogic = MemberLogic(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)
