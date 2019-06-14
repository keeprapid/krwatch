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

logging.config.fileConfig("/opt/Keeprapid/KRWatch/server/conf/log.conf")
logger = logging.getLogger('krwatch')


class GearCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(GearCenter, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("GearCenter :running in __init__")

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
        self.recv_queue_name = "W:Queue:GearCenter"
        if 'gearcenter' in _config:
            if 'Consumer_Queue_Name' in _config['gearcenter']:
                self.recv_queue_name = _config['gearcenter']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.zhijian_device
        self.collect_gearinfo = self.db.devices
        self.collect_siminfo = self.db.sims

        self.zhijianconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.zhijiandb = self.zhijianconn.zhijian
        self.collect_operatelogs = self.zhijiandb.operatelogs


    def run(self):
        logger.debug("Start GearCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
        try:
#        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logger.debug("_proc_message cost %f" % (time.time()-t1))
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))

#    def send_to_publish_queue(self, sendbuf):
#        '''sendbuf : dict'''
#        self._redis.lpush(self.publish_queue_name, json.dumps(sendbuf))

    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
        logger.debug('_proc_message')
        #解body
        msgdict = dict()
        try:
            logger.debug(recvbuf)
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

        msg_resp = json.dumps(message_resp_dict)
#        logger.debug(msg_resp)
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

        if action_cmd == 'gear_add':
            self._proc_action_gear_add(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'gear_add2':
            self._proc_action_gear_add2(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'sim_add1':
            self._proc_action_sim_add1(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'sim_add2':
            self._proc_action_sim_add2(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_gear_add(self, version, action_body, retdict, retbody):
        ''' input:
            vid  M   厂家编码 参见附录
            gear_type   M   设备类型编码 参见附录
            imei_start  M   
            imei_end    M  

            output:
            gear_count  O   添加的条数
            badimei     

            '''
        logger.debug("_proc_action_gear_add")
        #检查参数
        if action_body is None or  ('imei_start' not in action_body) or  ('imei_end' not in action_body) :
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'imei_start' not in action_body or action_body['imei_start'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'imei_end' not in action_body or action_body['imei_end'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return


        try:
            deviceinfodict = dict()
            user = ''
            for key in action_body:
                if key in ['imei_start','imei_end']:
                    continue
                if key in ['user']:
                    user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                    deviceinfodict[key] = user
                    continue
                deviceinfodict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

            imei_start = int(action_body['imei_start'])
            imei_end = int(action_body['imei_end'])

            if imei_start>imei_end:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            count = 0
            
            existimeilist = list()
            resultlist = self.collect_gearinfo.find({'imei':{'$gte':imei_start,'$lte':imei_end}})
            for gearinfo in resultlist:
                existimeilist.append(gearinfo['imei'])

#            logger.debug(existimeilist)
            for imei in xrange(imei_start, imei_end+1):
#                logger.debug(imei)
                if imei in existimeilist:
                    continue
                insertimei = dict({\
                    'imei':imei,\
                    'deviceid':str(imei),\
                    'createtime': datetime.datetime.now(),\
                    'activetime':None,\
                    'state': self.AUTH_STATE_NEW,\
                    'simnumber':'',\
                    'battery_level':0,\
                    'gsm_level':0,\
                    'last_location_type':0,\
                    'last_long':0,\
                    'last_lat':0,\
                    'last_vt':0,\
                    'last_angel':0,\
                    'last_location_objectid':None,\
                    'alarm_enable':1,\
                    'fence_list':dict(),\
                    'alert_motor':'1',\
                    'alert_ring':'1',\
                    'follow':''\
                    })
                insertimei.update(deviceinfodict)
                obj = self.collect_gearinfo.insert_one(insertimei)
                ret = obj.inserted_id
                if ret is not None:
                    insertimei['createtime'] = insertimei['createtime'].__str__()
#                    key = self.KEY_IMEI_ID % (str(imei), ret.__str__())
                    key = self.KEY_DEVICEINFO % (str(imei))
                    self._redis.hmset(key, insertimei)
                count += 1

                logdict = dict()
                logdict['user'] = user
                logdict['key'] = ret.__str__()
                logdict['table'] = 'devices'
                logdict['type'] = 'add'
                logdict['result'] = '1'
                logdict['time'] = datetime.datetime.now()
                self.collect_operatelogs.insert_one(logdict)

            retdict['error_code'] = '200'
            retbody['badmacid'] = ','.join(str(x) for x in existimeilist)
            retbody['gear_count'] = count

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return


    def _proc_action_gear_add2(self, version, action_body, retdict, retbody):
        ''' input:
            vid  M   厂家编码 参见附录
            gear_type   M   设备类型编码 参见附录
            imei_start  M   
            imei_end    M  

            output:
            gear_count  O   添加的条数
            badimei     

            '''
        logger.debug("_proc_action_gear_add2")
        #检查参数
        if action_body is None or ('imei_list' not in action_body) or ('vid' not in action_body):
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'imei_list' not in action_body or action_body['imei_list'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'vid' not in action_body or action_body['vid'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return


        try:
            deviceinfodict = dict()
            user = ''
            for key in action_body:
                if key in ['imei_list']:
                    continue
                if key in ['user']:
                    user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                    deviceinfodict[key] = user
                    continue
                deviceinfodict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

            tmpimei_list = action_body['imei_list'].split(',')
            count = 0

            imei_start = 0
            imei_end = 0
            imeilist = list()
            for imeistr in tmpimei_list:
                imei = int(imeistr)
                if imei_end == 0:
                    imei_end = imei
                if imei_start == 0:
                    imei_start = imei
                if imei>imei_end:
                    imei_end = imei
                if imei < imei_start:
                    imei_start = imei
                imeilist.append(imei)

            logger.debug("%d,%d" %(imei_start,imei_end))
            existimeilist = list()
            resultlist = self.collect_gearinfo.find({'imei':{'$gte':imei_start,'$lte':imei_end}})
            for gearinfo in resultlist:
                existimeilist.append(gearinfo['imei'])

            logger.debug(existimeilist)
            for imei in imeilist:
                logger.debug(imei)
                if imei in existimeilist:
                    continue
                insertimei = dict({\
                    'imei':imei,\
                    'deviceid':str(imei),\
                    'createtime': datetime.datetime.now(),\
                    'activetime':None,\
                    'state': self.AUTH_STATE_NEW,\
                    'simnumber':'',\
                    'battery_level':0,\
                    'gsm_level':0,\
                    'last_location_type':0,\
                    'last_long':0,\
                    'last_lat':0,\
                    'last_vt':0,\
                    'last_angel':0,\
                    'last_location_objectid':None,\
                    'alarm_enable':1,\
                    'fence_list':dict(),\
                    'alert_motor':'1',\
                    'alert_ring':'1',\
                    'follow':''\
                    })
                insertimei.update(deviceinfodict)
                obj = self.collect_gearinfo.insert_one(insertimei)
                ret = obj.inserted_id
                if ret is not None:
                    insertimei['createtime'] = insertimei['createtime'].__str__()
#                    key = self.KEY_IMEI_ID % (str(imei), ret.__str__())
                    key = self.KEY_DEVICEINFO % (str(imei))
                    self._redis.hmset(key, insertimei)
                count += 1

                logdict = dict()
                logdict['user'] = user
                logdict['key'] = ret.__str__()
                logdict['table'] = 'devices'
                logdict['type'] = 'add'
                logdict['result'] = '1'
                logdict['time'] = datetime.datetime.now()
                self.collect_operatelogs.insert_one(logdict)


            retdict['error_code'] = '200'
            retbody['badmacid'] = ','.join(str(x) for x in existimeilist)
            retbody['gear_count'] = count

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return


    def _proc_action_sim_add1(self, version, action_body, retdict, retbody):
        ''' input:
            vid  M   厂家编码 参见附录
            gear_type   M   设备类型编码 参见附录
            imei_start  M   
            imei_end    M  

            output:
            gear_count  O   添加的条数
            badimei     

            '''
        logger.debug("_proc_action_sim_add1")
        #检查参数
        if action_body is None or  ('sim_start' not in action_body) or  ('sim_end' not in action_body) or ('vid' not in action_body):
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'sim_start' not in action_body or action_body['sim_start'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'sim_end' not in action_body or action_body['sim_end'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'vid' not in action_body or action_body['vid'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return


        try:
            sininfodict = dict()
            user = ''
            for key in action_body:
                if key in ['sim_start','sim_end']:
                    continue
                if key in ['user']:
                    user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                    sininfodict[key] = user
                    continue
                sininfodict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

            sim_start = int(action_body['sim_start'])
            sim_end = int(action_body['sim_end'])

            if sim_start>sim_end:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            count = 0
            
            existimeilist = list()
            resultlist = self.collect_siminfo.find({'sim':{'$gte':sim_start,'$lte':sim_end}})
            for gearinfo in resultlist:
                existimeilist.append(gearinfo['sim'])

#            logger.debug(existimeilist)
            for sim in xrange(sim_start, sim_end+1):
#                logger.debug(sim)
                if sim in existimeilist:
                    continue
                insertimei = dict({\
                    'sim':sim,\
                    'createtime': datetime.datetime.now(),\
                    'activetime':None,\
                    'state': self.SIM_STATE_NEW,\
                    'simnumber':str(sim)\
                    })
                insertimei.update(sininfodict)
                obj = self.collect_siminfo.insert_one(insertimei)
                ret = obj.inserted_id
                count += 1

                logdict = dict()
                logdict['user'] = user
                logdict['key'] = ret.__str__()
                logdict['table'] = 'sims'
                logdict['type'] = 'add'
                logdict['result'] = '1'
                logdict['time'] = datetime.datetime.now()
                self.collect_operatelogs.insert_one(logdict)



            retdict['error_code'] = '200'
            retbody['badsim'] = ','.join(str(x) for x in existimeilist)
            retbody['gear_count'] = count

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return


    def _proc_action_sim_add2(self, version, action_body, retdict, retbody):
        ''' input:
            vid  M   厂家编码 参见附录
            gear_type   M   设备类型编码 参见附录
            imei_start  M   
            imei_end    M  

            output:
            gear_count  O   添加的条数
            badimei     

            '''
        logger.debug("_proc_action_sim_add2")
        #检查参数
        if action_body is None or  ('sim_list' not in action_body) or ('vid' not in action_body):
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'sim_list' not in action_body or action_body['sim_list'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'vid' not in action_body or action_body['vid'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return


        try:
            sininfodict = dict()
            user = ''
            for key in action_body:
                if key in ['sim_list']:
                    continue
                if key in ['user']:
                    user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                    sininfodict[key] = user
                    continue
                sininfodict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

            vid = action_body['vid']
            tmpimei_list = action_body['sim_list'].split(',')
            count = 0

            imei_start = 0
            imei_end = 0
            imeilist = list()
            for imeistr in tmpimei_list:
                imei = int(imeistr)
                if imei_end == 0:
                    imei_end = imei
                if imei_start == 0:
                    imei_start = imei
                if imei>imei_end:
                    imei_end = imei
                if imei < imei_start:
                    imei_start = imei
                imeilist.append(imei)

            existimeilist = list()
            resultlist = self.collect_siminfo.find({'sim':{'$gte':imei_start,'$lte':imei_end}})
            for gearinfo in resultlist:
                existimeilist.append(gearinfo['sim'])

#            logger.debug(existimeilist)
            for sim in imeilist:
#                logger.debug(imei)
                if sim in existimeilist:
                    continue
                insertimei = dict({\
                    'sim':sim,\
                    'createtime': datetime.datetime.now(),\
                    'activetime':None,\
                    'state': self.SIM_STATE_NEW,\
                    'simnumber':str(sim)\
                    })
                insertimei.update(sininfodict)
                obj = self.collect_siminfo.insert_one(insertimei)
                ret = obj.inserted_id
                count += 1

                logdict = dict()
                logdict['user'] = user
                logdict['key'] = ret.__str__()
                logdict['table'] = 'sims'
                logdict['result'] = '1'
                logdict['type'] = 'add'
                logdict['time'] = datetime.datetime.now()
                self.collect_operatelogs.insert_one(logdict)


            retdict['error_code'] = '200'
            retbody['badsim'] = ','.join(str(x) for x in existimeilist)
            retbody['gear_count'] = count

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return


if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'gearcenter' in _config and _config['gearcenter'] is not None:
        if 'thread_count' in _config['gearcenter'] and _config['gearcenter']['thread_count'] is not None:
            thread_count = int(_config['gearcenter']['thread_count'])

    for i in xrange(0, thread_count):
        GearCenter = GearCenter(i)
        GearCenter.setDaemon(True)
        GearCenter.start()

    while 1:
        time.sleep(1)
