#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  Session_worker.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo Session 工作线程

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


class Session(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(Session, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("Session :running in __init__")

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
        self.recv_queue_name = "W:Queue:Session"
        if 'session' in _config:
            if 'Consumer_Queue_Name' in _config['session']:
                self.recv_queue_name = _config['session']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.location
        self.collect_lbsinfo = self.db.lbsinfo
        self.collect_gpsinfo = self.db.gpsinfo

        self.memberconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.memberdb = self.memberconn.zhijian_member
        self.collect_memberinfo = self.memberdb.members
        self.collect_fenceinfo = self.memberdb.alarms

        self.notifyconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.notifydb = self.notifyconn.notify
        self.collect_notifytrigger = self.notifydb.notify_trigger
        self.collect_notifylog = self.notifydb.notify_log

    def run(self):
        logger.debug("Start Session pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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
        logger.debug('action_cmd : %s' % (action_cmd))
        action_version = msg_in['version']
        logger.debug('action_version : %s' % (action_version))
        if 'body' in msg_in:
            action_body = msg_in['body']
#            logger.debug('action_body : %s' % (action_body))
        else:
            action_body = None
            logger.debug('no action_body')

        if action_cmd == 'check_undo_msg':
            self._proc_action_check_undo_msg(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'check_location':
            self._proc_action_check_location(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'check_notify_logic':
            self._proc_action_check_notify_logic(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_check_undo_msg(self, version, action_body, retdict, retbody):
        ''' input:
            vid  M   厂家编码 参见附录
            gear_type   M   设备类型编码 参见附录
            imei_start  M   
            imei_end    M  

            output:
            gear_count  O   添加的条数
            badimei     

            '''
        logger.debug("_proc_action_check_undo_msg")
        #检查参数
        if action_body is None or ('imei' not in action_body):
            logger.error("mandotry param error in action")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return
        if 'imei' not in action_body or action_body['imei'] is None:
            logger.error("mandotry param error in action")
            retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
            return

        try:
            imei_str = action_body['imei']
            searchkey = self.KEY_IMEI_CMD % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                for key in resultlist:
                    cmdinfo = self._redis.hgetall(key)
                    if 'topic' in cmdinfo and 'sendbuf' in cmdinfo:
                        senddict = dict()
                        senddict['topic'] = cmdinfo['topic']
                        senddict['sendbuf'] = cmdinfo['sendbuf']
                        if 'mqtt_publish' in self._config:
                            self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))
                            #休息0.1秒
                            time.sleep(0.1)
                    elif 'topic' in cmdinfo and 'content' in cmdinfo and 'clock_time' in cmdinfo:
                        senddict = dict()
                        senddict['topic'] = cmdinfo['topic']
                        senddict['content'] = cmdinfo['content']
                        senddict['clock_time'] = cmdinfo['clock_time']
                        if 'mqtt_publish' in self._config:
                            self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))
                            #休息0.1秒
                            time.sleep(0.1)




        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return

    def _proc_action_check_notify_logic(self, version, action_body, retdict, retbody):
        logger.debug(" into _proc_action_check_notify_logic action_body:%s"%(action_body))
        try:
            imei = action_body.get('imei')
            deviceinfokey = self.KEY_DEVICEINFO % (imei)
            deviceinfo = self._redis.hgetall(deviceinfokey)
            needOOSNotify = False
            needHealthNotify = False

            if deviceinfo is None:
                logger.error("No imei cache")
                return
            memberid = deviceinfo.get('follow')
            if memberid is None or memberid == '':
                logger.error("Member [%s] is not EXIST!!!",memberid)
                return            
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                logger.error("Member [%s] is not EXIST!!!",memberid)
                return

            memberstate = memberinfo.get('state')
            if memberstate != self.MEMBER_STATE_SURVEILLANCE:
                triggerinfo = self.collect_notifytrigger.find_one({'type':self.NOTIFY_TRIGGER_TYPE_SURVCLOSE})
                if triggerinfo is not None and triggerinfo.get('state') == 1:
                    interval = triggerinfo.get('interval')
                    intervalkey = self.KEY_TRIGGER_IMEI_NOTIFY_TIMEOUT%(imei,self.NOTIFY_TRIGGER_TYPE_SURVCLOSE)
                    if self._redis.exists(intervalkey) is False:
                        logger.debug("[%s] trigger notify [%s]"%(imei,triggerinfo.get('type')))
                        #在此时给设备下发通知
                        devicelang = deviceinfo.get('language')
                        if devicelang is None:
                            devicelang = 'chs'
                        contentkey = 'content_'+devicelang
                        if contentkey not in triggerinfo:
                            contentkey = 'content_chs'
                        content = triggerinfo.get(contentkey)
                        if content is not None and len(content)>0:
                            body = dict()
                            body['imei'] = imei
                            body['content'] = content
                            action = dict()
                            action['body'] = body
                            action['version'] = '1.0'
                            action['action_cmd'] = 'push_devicemessage'
                            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                            action['from'] = ''
                            if 'mtpdatacenter' in self._config:
                                self._sendMessage(self._config['mtpdatacenter']['Consumer_Queue_Name'], json.dumps(action))
                        insertlog = dict()
                        insertlog['deviceid'] = imei
                        insertlog['memberid'] = memberid
                        insertlog['triggertype'] = triggerinfo.get('type')
                        insertlog['content'] = content
                        insertlog['timestamp'] = datetime.datetime.now()
                        self.collect_notifylog.insert_one(insertlog)
                        self._redis.set(intervalkey,'1')
                        self._redis.expire(intervalkey,interval)
            else:
                #检查别的业务
                healthkey = self.KEY_MEMBER_HEALTHSTATE % (memberid)
                if self._redis.exists(healthkey) is False:
                    triggerinfo = self.collect_notifytrigger.find_one({'type':self.NOTIFY_TRIGGER_TYPE_HEALTHTIMEOUT})
                    if triggerinfo is not None and triggerinfo.get('state') == 1:
                        interval = triggerinfo.get('interval')
                        intervalkey = self.KEY_TRIGGER_IMEI_NOTIFY_TIMEOUT%(imei,self.NOTIFY_TRIGGER_TYPE_HEALTHTIMEOUT)
                        if self._redis.exists(intervalkey) is False:
                            #在此时给设备下发通知
                            logger.debug("[%s] trigger notify [%s]"%(imei,triggerinfo.get('type')))
                            devicelang = deviceinfo.get('language')
                            if devicelang is None:
                                devicelang = 'chs'
                            contentkey = 'content_'+devicelang
                            if contentkey not in triggerinfo:
                                contentkey = 'content_chs'
                            content = triggerinfo.get(contentkey)
                            if content is not None and len(content)>0:
                                body = dict()
                                body['imei'] = imei
                                body['content'] = content
                                action = dict()
                                action['body'] = body
                                action['version'] = '1.0'
                                action['action_cmd'] = 'push_devicemessage'
                                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                                action['from'] = ''
                                if 'mtpdatacenter' in self._config:
                                    self._sendMessage(self._config['mtpdatacenter']['Consumer_Queue_Name'], json.dumps(action))
                            insertlog = dict()
                            insertlog['deviceid'] = imei
                            insertlog['memberid'] = memberid
                            insertlog['triggertype'] = triggerinfo.get('type')
                            insertlog['content'] = content
                            insertlog['timestamp'] = datetime.datetime.now()
                            self.collect_notifylog.insert_one(insertlog)
                            self._redis.set(intervalkey,'1')
                            self._redis.expire(intervalkey,interval)

            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL



    def _proc_action_check_location(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
            body['imei'] = imei
            body['latitude'] = latitude
            body['longitude'] = longitude
            body['imeikey'] = imeikey
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_check_location action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('latitude' not in action_body) or  ('longitude' not in action_body) or  ('memberid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['latitude'] is None or action_body['latitude'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['longitude'] is None or action_body['longitude'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['memberid'] is None or action_body['memberid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            memberid = action_body['memberid']
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return
                
            if isinstance(action_body['latitude'], float):
                latitude = action_body['latitude']
            else:
                latitude = float(action_body['latitude'].replace('N','').replace('S',''))
                
            if isinstance(action_body['longitude'], float):
                longitude = action_body['longitude']
            else:
                longitude = float(action_body['longitude'].replace('E','').replace('W',''))

            if 'fence_name' not in memberinfo or memberinfo['fence_name'] is None:
                retdict['error_code'] = self.ERRORCODE_IMEI_HAS_NO_FENCE
                return

            fenceinfo = self.collect_fenceinfo.find_one({'fence_name':memberinfo.get('fence_name')})
            if fenceinfo is None:
                retdict['error_code'] = self.ERRORCODE_IMEI_HAS_NO_FENCE
                return

#            fence_list = memberinfo.get('fence_list')
#            if len(fence_list)== 0:
#                retdict['error_code'] = self.ERRORCODE_IMEI_HAS_NO_FENCE
#                return
#            fencekey = fence_list.keys()[0]
#            fenceinfo = fence_list.get(fencekey)
            fenceenable = fenceinfo.get('fence_enable')
            if fenceenable != 1:
                retdict['error_code'] = self.ERRORCODE_IMEI_FENCE_DEACTIVE
                return

            fencedirection = fenceinfo.get('fence_direction')

            coordlist = fenceinfo.get('coord_list')
            points = list()
            for obj in coordlist:
                point = list()
                point.append(obj.get('latitude'))
                point.append(obj.get('longitude'))
                points.append(point)

            ret = self.is_point_in(latitude, longitude, points)
            logger.debug(ret)
            logger.debug(fencedirection)
            if fencedirection == 'out':
                if ret is False:
                    logger.debug(" notify user when ret is false, out fence")
                    #通知用户
                    body = dict()
                    body['username'] = memberinfo['username']
                    body['memberid'] = memberid
                    body['type'] = self.ALARM_TYPE_FENCE
                    body['value'] = fencedirection
                    body['latitude'] = latitude
                    body['longitude'] = longitude
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
                    body = dict()
                    body['memberid'] = memberid
                    body['type'] = self.ALARM_TYPE_FENCE
                    action = dict()
                    action['body'] = body
                    action['version'] = '1.0'
                    action['action_cmd'] = 'alarm_close'
                    action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                    action['from'] = ''
                    self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))

            else:
                if ret is True:
                    #通知用户
                    logger.debug(" notify user when ret is true, in fence")
                    body = dict()
                    body['username'] = memberinfo['username']
                    body['memberid'] = memberid
                    body['type'] = self.ALARM_TYPE_FENCE
                    body['value'] = fencedirection
                    body['latitude'] = latitude
                    body['longitude'] = longitude
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
                    body = dict()
                    body['memberid'] = memberid
                    body['type'] = self.ALARM_TYPE_FENCE
                    action = dict()
                    action['body'] = body
                    action['version'] = '1.0'
                    action['action_cmd'] = 'alarm_close'
                    action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                    action['from'] = ''
                    self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))



            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def is_point_in(self, x, y, points):
        count = 0
        x1, y1 = points[0]
        x1_part = (y1 > y) or ((x1 - x > 0) and (y1 == y)) # x1在哪一部分中
        x2, y2 = '', ''  # points[1]
        points.append((x1, y1))
        for point in points[1:]:
            x2, y2 = point
            x2_part = (y2 > y) or ((x2 > x) and (y2 == y)) # x2在哪一部分中
            if x2_part == x1_part:
                x1, y1 = x2, y2
                continue
            mul = (x1 - x)*(y2 - y) - (x2 - x)*(y1 - y)
            if mul > 0:  # 叉积大于0 逆时针
                count += 1
            elif mul < 0:
                count -= 1
            x1, y1 = x2, y2
            x1_part = x2_part
        if count == 2 or count == -2:
            return True
        else:
            return False

if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'session' in _config and _config['session'] is not None:
        if 'thread_count' in _config['session'] and _config['session']['thread_count'] is not None:
            thread_count = int(_config['session']['thread_count'])

    for i in xrange(0, thread_count):
        session = Session(i)
        session.setDaemon(True)
        session.start()

    while 1:
        time.sleep(1)
