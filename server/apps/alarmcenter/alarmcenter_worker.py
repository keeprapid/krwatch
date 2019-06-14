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


class AlarmCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(MemberLogic, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("AlarmCenter :running in __init__")

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
        self.recv_queue_name = "W:Queue:AlarmCenter"
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        if 'alarmcenter' in self._config:
            if 'Consumer_Queue_Name' in self._config['alarmcenter']:
                self.recv_queue_name = self._config['alarmcenter']['Consumer_Queue_Name']

        self.zhijianconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.zhijiandb = self.zhijianconn.zhijian
        self.col_alarmlogs = self.zhijiandb.alarmlogs

        self.memberconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.memberdb = self.memberconn.zhijian_member
        self.col_memberinfo = self.memberdb.members

        self.publish_queue_name = "W:Queue:MQTTPub"
        if 'mqtt_publish' in self._config:
            if 'Consumer_Queue_Name' in self._config['mqtt_publish']:
                self.publish_queue_name = self._config['mqtt_publish']['Consumer_Queue_Name']


    def run(self):
        logger.debug("Start AlarmCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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
        logger.debug("::::::RetBuf--->errorcode = %s " % (message_resp_dict.get('error_code')))

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

        if action_cmd == 'alarm_create':
            self._proc_action_alarm_create(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'alarm_close':
            self._proc_action_alarm_close(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_alarm_create(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'alarm_create', M
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
        '''
        logger.debug(" into _proc_action_alarm_create action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('type' not in action_body) or  ('value' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['type'] is None or action_body['type'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_M_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            if action_body['value'] is None or action_body['value'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            #写入alarm表
            insertlog = dict()
            mqttdict = dict()
            for key in action_body:
                if key in ['username']:
                    mqttdict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                    insertlog[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                elif key in['last_location_time']:
                    mqttdict[key] = action_body[key]
                    insertlog[key] = datetime.datetime.strptime(action_body[key],'%Y-%m-%d %H:%M:%S')
                else:
                    mqttdict[key] = action_body[key]
                    insertlog[key] = action_body[key]


            insertlog['timestamp'] = datetime.datetime.now()
            logger.debug(insertlog)
            self.col_alarmlogs.insert_one(insertlog)
            memberinfo = self.col_memberinfo.find_one({'username':action_body['username']})
            if memberinfo is None:
                logger.error("[%s] memberinfo not exist"%(action_body['username']))
                return
            alarminfo = memberinfo.get('alarminfo')
            if alarminfo is None:
                alarminfo = ''

            alarminfolist = set(alarminfo.split(','))
            if action_body['type'] not in alarminfolist:
                alarminfolist.add(action_body['type'])

            if self.ALARM_TYPE_NORMAL in alarminfolist:
                #有告警信息后就要将正常的状态删除
                alarminfolist.remove(self.ALARM_TYPE_NORMAL)

            updatedict = dict()
            updatedict['alarminfo'] = ','.join(alarminfolist)
            updatedict['last_alarm_time'] = datetime.datetime.now()
            self.col_memberinfo.update_one({'username':action_body['username']},{'$set':updatedict})
            # #发出alarm mqtt消息
            # senddict = dict()
            # senddict['topic'] = self._mqttconfig['mqtt_alarm_topic_prefix']
            # senddict['sendbuf'] = json.dumps(mqttdict)
            # self._redis.lpush(self.publish_queue_name, json.dumps(senddict))
            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_alarm_close(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'alarm_close', M
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
        '''
        logger.debug(" into _proc_action_alarm_close action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('memberid' not in action_body) or  ('type' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['type'] is None or action_body['type'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_M_INVALID
                return
            if action_body['memberid'] is None or action_body['memberid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            #写入alarm表
            memberid = action_body['memberid']
            alarmtype = action_body['type']

            memberinfo = self.col_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                logger.error("[%s] memberinfo not exist"%(action_body['username']))
                return
            alarminfo = memberinfo.get('alarminfo')
            if alarminfo is None:
                alarminfo = self.ALARM_TYPE_NORMAL

            alarminfolist = set(alarminfo.split(','))
            if alarmtype in alarminfolist:
                alarminfolist.remove(alarmtype)

            if len(alarminfolist) == 0:
                alarminfolist.add(self.ALARM_TYPE_NORMAL)

            updatedict = dict()
            updatedict['alarminfo'] = ','.join(alarminfolist)
            self.col_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':updatedict})
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
    if _config is not None and 'alarmcenter' in _config and _config['alarmcenter'] is not None:
        if 'thread_count' in _config['alarmcenter'] and _config['alarmcenter']['thread_count'] is not None:
            thread_count = int(_config['alarmcenter']['thread_count'])

    for i in xrange(0, thread_count):
        alarmcenter = AlarmCenter(i)
        alarmcenter.setDaemon(True)
        alarmcenter.start()

    while 1:
        time.sleep(1)
