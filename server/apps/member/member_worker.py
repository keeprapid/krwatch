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
        self.recv_queue_name = "W:Queue:Member"
        if 'member' in _config:
            if 'Consumer_Queue_Name' in _config['member']:
                self.recv_queue_name = _config['member']['Consumer_Queue_Name']

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
        self.colgps = self.locdb.gpsinfo
        self.collbs = self.locdb.lbsinfo

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



    def calcpassword(self, password, verifycode):
        m0 = hashlib.md5(verifycode)
        logger.debug("m0 = %s" % m0.hexdigest())
        m1 = hashlib.md5(password + m0.hexdigest())
    #        print m1.hexdigest()
        logger.debug("m1 = %s" % m1.hexdigest())
        md5password = m1.hexdigest()
        return md5password

    def generator_tokenid(self, userid, timestr, verifycode):
        m0 = hashlib.md5(verifycode)
    #        print m0.hexdigest()
        m1 = hashlib.md5("%s%s%s" % (userid,timestr,m0.hexdigest()))
    #        print m1.hexdigest()
        token = m1.hexdigest()
        return token

    def run(self):
        logger.debug("Start MemberLogic pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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

        if action_cmd == 'register':
            self._proc_action_register(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'login':
            self._proc_action_login(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'logout':
            self._proc_action_logout(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_info':
            self._proc_action_member_info(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_active':
            self._proc_action_member_active(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'change_pasiwode':
#            self._proc_action_change_pasiwode(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'getback_pasiwode':
#            self._proc_action_getback_pasiwode(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_add_gear':
#            self._proc_action_member_add_gear(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_query_gear':
#            self._proc_action_member_query_gear(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_update_gear':
#            self._proc_action_member_update_gear(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_gear_add_fence':
#            self._proc_action_member_gear_add_fence(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_del_gear':
#            self._proc_action_member_del_gear(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_gear_headimg_upload':
#            self._proc_action_member_gear_headimg_upload(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_update_fence_notify':
#            self._proc_action_member_update_fence_notify(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'gear_reset':
#            self._proc_action_gear_reset(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'reset_pasiwode':
#            self._proc_action_reset_pasiwode(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_add_gear2':
#            self._proc_action_member_add_gear2(action_version, action_body, msg_out_head, msg_out_body)
#        elif action_cmd == 'member_email':
#            self._proc_action_member_email(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_declare_req':
            self._proc_action_member_declare_req(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_declare':
            self._proc_action_member_declare(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_gps_upload':
            self._proc_action_member_gps_upload(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_login(self, version, action_body, retdict, retbody):
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
        '''
        logger.debug(" into _proc_action_login action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            vid = action_body['vid'];
            #在redis中查找用户登陆信息
            tokenid = None
            nickname = ""
            memberid = ''
            addkey = ''

            memberinfo = self.collect_memberinfo.find_one({'username': username})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return
            else:
                memberid = memberinfo['_id'].__str__()
                if action_body['pwd'] != memberinfo['password']:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                    return

                memberinfokey = self.KEY_MEMBERINFO % (memberid)
                if self._redis.exists(memberinfokey):
                    oldtokenid = self._redis.hget(memberinfokey,'token')
                    if oldtokenid is not None:
                        oldtokenkey = self.KEY_TOKEN % (oldtokenid)
                        self._redis.delete(oldtokenkey)
                tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self._json_dbcfg.get("verifycode"))
                tokenkey = self.KEY_TOKEN % (tokenid)
                tokendict = dict()
                tokendict['token'] = tokenid
                tokendict['memberid'] = memberid
                self._redis.hmset(tokenkey, tokendict)
                self._redis.expire(tokenkey,self.MONITOR_TIMEOUT)
                updatedict = dict()
                updatedict.update(memberinfo)
                updatedict['token'] = tokenid
                for key in updatedict:
                    if key in '_id':
                        updatedict[key] = updatedict[key].__str__()
                    elif isinstance(updatedict[key],datetime.datetime):
                        updatedict[key] = updatedict[key].__str__()
                self._redis.hmset(memberinfokey,updatedict)
                logger.debug("tokendict = %r" % tokendict)


#            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username,'*')
#            logger.debug("key = %s" % searchkey)
#            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % resultlist)
#
#            if len(resultlist):
#                redis_memberinfo = self._redis.hgetall(resultlist[0])
#                if redis_memberinfo is None:
#                    #redis中没有用户信息，就从mongo中读取
#                    member = self.collect_memberinfo.find_one({'username': username})
#                    if member is None:
#                        retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
#                        return
#                    #比较密码
#                    if action_body['pwd'] != member['pasiwode']:
#                        retdict['error_code'] = self.ERRORCODE_MEMBER_pasiwode_INVALID
#                        return
#                    #产生tokenid
#                    memberid = member['_id'].__str__()
#                    tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self._json_dbcfg.get("verifycode"))
#                    #写入redis
#                    addkey = self.KEY_TOKEN_NAME_ID % (tokenid,member['username'], memberid)
#                    self._redis.hset(addkey, 'tid', tokenid)
#                    for key in member:
#                        if key == '_id':
#                            self._redis.hset(addkey, key, memberid)
#                        elif key == 'tid':
#                            continue
#                        elif key in ['createtime', 'lastlogintime','activetime']:
#                            self._redis.hset(addkey, key, member[key].__str__())
#                        else:
#                            self._redis.hset(addkey, key, member[key])
#                    #删除无用的key
#                    self._redis.delete(resultlist[0])
#                    nickname = member.get('nickname')
#
#                else:
#                    memberid = redis_memberinfo['_id']
#                    tokenid = redis_memberinfo['tid']
#                    pasiwode = redis_memberinfo['pasiwode']
#                    nickname = redis_memberinfo.get('nickname')
#                    addkey = resultlist[0]
#                    #比较密码
#                    if action_body['pwd'] != pasiwode:
#                        retdict['error_code'] = self.ERRORCODE_MEMBER_pasiwode_INVALID
#                        return
#            else:
#                member = None
#                member = self.collect_memberinfo.find_one({'username': username})
#
#                if member is None:
#                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
#                    return
#                #比较密码
#                if action_body['pwd'] != member['pasiwode']:
#                    retdict['error_code'] = self.ERRORCODE_MEMBER_pasiwode_INVALID
#                    return
#
#                memberid = member['_id'].__str__()
#                nickname = member.get('nickname')
#
#                searchkey = self.KEY_TOKEN_NAME_ID % ('*', member['username'], member['_id'].__str__())
#                resultlist = self._redis.keys(searchkey)
#                if len(resultlist)>0:
#                    memberinfo = self._redis.hgetall(resultlist[0])
#                    tokenid = memberinfo.get('tid')
#                else:
#                #产生tokenid
#                    tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self._json_dbcfg.get("verifycode"))
#                #写入redis
#                    addkey = self.KEY_TOKEN_NAME_ID % (tokenid,member['username'], memberid)
#                    self._redis.hset(addkey, 'tid', tokenid)
#                    for key in member:
#                        if key == '_id':
#                            self._redis.hset(addkey, key, memberid)
#                        elif key == 'tid':
#                            continue
#                        elif key in ['createtime', 'lastlogintime', 'activetime']:
#                            self._redis.hset(addkey, key, member[key].__str__())
#                        else:
#                            self._redis.hset(addkey, key, member[key])
#

            #更新上次登陆时间
            t = datetime.datetime.now()
            self._redis.hset(addkey, 'lastlogintime', t.__str__())
            self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':{'lastlogintime':t}})


            if 'phone_name' in action_body and action_body['phone_name'] is not None:
                phone_name = urllib.unquote(action_body['phone_name'].encode('utf-8')).decode('utf-8')
            else:
                phone_name = ''
            if 'phone_os' in action_body and action_body['phone_os'] is not None:
                phone_os = urllib.unquote(action_body['phone_os'].encode('utf-8')).decode('utf-8')
            else:
                phone_os =  ''
            if 'app_version' in action_body and action_body['app_version'] is not None:
                app_version = action_body['app_version']
            else:
                app_version =  ''
            if 'phone_id' in action_body and action_body['phone_id'] is not None:
                phone_id = urllib.unquote(action_body['phone_id'].encode('utf-8')).decode('utf-8')
            else:
                phone_id =  ''
            #添加登陆记录
            insertlog = dict({\
                'mid':memberid,\
                'tid':tokenid,\
                'vid':vid,\
                'phone_id': phone_id,\
                'phone_name': phone_name,\
                'phone_os': phone_os,\
                'app_version': app_version,\
                'timestamp': datetime.datetime.now()\
                })
            logger.debug(insertlog)
            self.collect_memberlog.insert_one(insertlog)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['tid'] = tokenid
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_logout(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_logout action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            ios_token = None
            if 'ios_token' in action_body and action_body['ios_token'] is not None:
                ios_token = action_body['ios_token']

            tid = action_body['tid']
            tokenkey = self.KEY_TOKEN % (tid)
            if self._redis.exists(tokenkey):
                memberid = self._redis.hget(tokenkey,'memberid')
                if memberid is not None:
                    memberinfokey = self.KEY_MEMBERINFO % (memberid)
                    if self._redis.exists(memberinfokey):
                        oldtoken = self._redis.hget(memberinfokey,'token')
                        if oldtoken is not None and oldtoken == tid:
                            self._redis.hset(memberinfokey,'token','')

                self._redis.delete(tokenkey)

#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
#            if len(resultlist):
#                memberinfo = self._redis.hgetall(resultlist[0])
#                if 'userprofile' in memberinfo:
#                    userprofile = eval(memberinfo.get('userprofile'))
#                    if 'ios_token' in userprofile:
#                        if ios_token is not None and ios_token in userprofile['ios_token']:
#                            logger.debug("delete ios_tokenid")
#                            userprofile['ios_token'].remove(ios_token)
#
#                        if len(userprofile['ios_token']) >0:
#                            logger.debug("update ios_tokenid")
#                            self._redis.hset(resultlist[0],'userprofile',userprofile)
#
#                        else:
#                            self.redisdelete(resultlist)
#                    else:
#                        self.redisdelete(resultlist)
#                else:
#                    self.redisdelete(resultlist)

            retdict['error_code'] = '200'
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_register(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_register action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('vid' not in action_body) or ('deviceid' not in action_body) or ('simnumber' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['username'] is None or action_body['vid'] is None or action_body['deviceid'] is None or action_body['simnumber'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            user = ''
            deviceid = action_body['deviceid']
            simnumber = action_body['simnumber']
            tid = action_body['tid']
            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            if len(username)<6:
                retdict['error_code'] = self.ERRORCODE_MEMBER_ID_INVALID
                return

            password = username[-6:]
            password = self.calcpassword(password, self._json_dbcfg.get("verifycode"))

            memberinfo = self.collect_memberinfo.find_one({'username':username})
            activateduration = 0
            location_timeout = self.DEFAULT_LOCATION_TIMEOUT
            updateredisdict = dict()

            if memberinfo is not None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_USER_ALREADY_EXIST
                return
            if deviceid != '':
                imei = int(deviceid)
                deviceinfo = self.collect_gearinfo.find_one({'deviceid':deviceid})
                if deviceinfo is None:
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return

                # siminfo = self.collect_siminfo.find_one({'simnumber':simnumber})
                # if siminfo is None:
                #     retdict['error_code'] = self.ERRORCODE_SIMNUMBER_NOT_EXIST
                #     return

                if 'follow' in deviceinfo and deviceinfo['follow']!= '':
                    retdict['error_code'] = self.ERRORCODE_IMEI_HAS_OWNER
                    return

                # if 'deviceid' in siminfo and siminfo['deviceid'] != '':
                #     retdict['error_code'] = self.ERRORCODE_SIMNUMBER_ALREADY_USED
                #     return

                insertdict = dict()
                for key in action_body:
                    if key in ['user']:
                        user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                        insertdict[key] = user
                        continue
                    elif key in ['activateduration','location_timeout']:
                        insertdict[key] = action_body[key]
                    else:
                        insertdict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

                insertdict['createtime'] = datetime.datetime.now()
                insertdict['device_objid'] = deviceinfo['_id'].__str__()
                insertdict['activetime'] = datetime.datetime.now()
                insertdict['password'] = password
                insertdict['alarm_enable'] = '1'
                insertdict['fence_list'] = dict()
                insertdict['state'] = self.MEMBER_STATE_SURVEILLANCE
                insertdict['alarminfo'] = self.ALARM_TYPE_NORMAL
                if 'activateduration' not in insertdict or insertdict['activateduration'] is None:
                    insertdict['activateduration'] = self.MONITOR_TIMEOUT
                    activateduration = self.MONITOR_TIMEOUT
                else:
                    activateduration = insertdict['activateduration'] 

                if 'location_timeout' not in insertdict or insertdict['location_timeout'] is None:
                    insertdict['location_timeout'] = self.DEFAULT_LOCATION_TIMEOUT
                    location_timeout = self.DEFAULT_LOCATION_TIMEOUT
                else:
                    location_timeout = insertdict['location_timeout'] 

                #测试
#                activateduration = 90
#                location_timeout = 30
#                insertdict['activateduration'] = activateduration
#                insertdict['location_timeout'] = location_timeout

                # logger.debug(insertdict)
                updateredisdict.update(insertdict)
                obj = self.collect_memberinfo.insert_one(insertdict)
                insertMemberObjectId = obj.inserted_id

#                self.collect_gearinfo.update({'imei':imei},{'$set':{'follow':insertMemberObjectId.__str__()}})
                self.collect_gearinfo.update_one({'imei':imei},{'$set':{'follow':insertMemberObjectId.__str__(),'simnumber':simnumber,'activetime':datetime.datetime.now(),'state':self.AUTH_STATE_BIND,'username':username}})
#                followlist = list()
#                followlist.append(insertMemberObjectId.__str__())
                deviceinfo['follow'] = insertMemberObjectId.__str__()
                deviceinfo['simnumber'] = simnumber
                deviceinfo['state'] = self.AUTH_STATE_BIND
                deviceinfo['username'] = username
                deviceinfo['activetime'] = datetime.datetime.now().__str__()

#                gearmemberkey = self.KEY_IMEI_MNAME_MID % (deviceid,username,insertMemberObjectId.__str__())
#                self._redis.hmset(gearmemberkey,deviceinfo)
#                self._redis.expire(gearmemberkey,self.MONITOR_TIMEOUT)
                deviceinfokey = self.KEY_DEVICEINFO % (deviceid)
                self._redis.hmset(deviceinfokey,deviceinfo)
                self._redis.expire(deviceinfokey,self.MONITOR_TIMEOUT)

                setkey = self.KEY_DEVICESUVKEY % (deviceid)
                self._redis.set(setkey,'1')
                self._redis.expire(setkey,self.MONITOR_TIMEOUT)

                # self.collect_siminfo.update_one({'simnumber':simnumber},{'$set':{'deviceid':deviceid,'username':username,'device_objid':deviceinfo['_id'].__str__(),'memberid':insertMemberObjectId.__str__(),'state':self.SIM_STATE_BIND,'activetime':datetime.datetime.now()}})

                # #更新sim卡表
                # simupdatedict = dict()
                # simupdatedict['deviceid'] = deviceid
                # simupdatedict['memberid'] = insertMemberObjectId.__str__()
                # simupdatedict['username'] = username
                # simupdatedict['devicetype'] = action_body.get('devicetype')
                # simupdatedict['activatetime'] = datetime.datetime.now()
                # simupdatedict['state'] = self.SIM_STATE_BIND
                # self.collect_siminfo.update_one({'simnumber':simnumber},{'$set':simupdatedict})

                loglist = list()
                logmember = dict()
                logmember['user'] = user
                logmember['key'] = insertMemberObjectId.__str__()
                logmember['table'] = 'members'
                logmember['result'] = '1'
                logmember['type'] = 'add'
                logmember['time'] = datetime.datetime.now()
                loglist.append(logmember)

                loggear = dict()
                loggear['user'] = user
                loggear['key'] = deviceinfo['_id'].__str__()
                loggear['table'] = 'devices'
                loggear['result'] = '1'
                loggear['type'] = 'upate'
                loggear['time'] = datetime.datetime.now()
                loglist.append(loggear)

                # logsim = dict()
                # logsim['user'] = user
                # logsim['key'] = siminfo['_id'].__str__()
                # logsim['table'] = 'sims'
                # logsim['result'] = '1'
                # logsim['type'] = 'update'
                # logsim['time'] = datetime.datetime.now()
                # loglist.append(logsim)
                self.col_operatelogs.insert_many(loglist)
            else:
                insertdict = dict()
                for key in action_body:
                    if key in ['user']:
                        user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                        continue
                    elif key in ['activateduration','location_timeout']:
                        insertdict[key] = action_body[key]
                    else:
                        insertdict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

                insertdict['createtime'] = datetime.datetime.now()
                insertdict['device_objid'] = ''
                insertdict['activetime'] = datetime.datetime.now()
#                insertdict['activateduration'] = self.MONITOR_TIMEOUT
                insertdict['password'] = password
                insertdict['state'] = self.MEMBER_STATE_SURVEILLANCE
                if 'activateduration' not in insertdict or insertdict['activateduration'] is None:
                    insertdict['activateduration'] = self.MONITOR_TIMEOUT
                    activateduration = self.MONITOR_TIMEOUT
                else:
                    activateduration = insertdict['activateduration'] 

                if 'location_timeout' not in insertdict or insertdict['location_timeout'] is None:
                    insertdict['location_timeout'] = self.DEFAULT_LOCATION_TIMEOUT
                    location_timeout = self.DEFAULT_LOCATION_TIMEOUT
                else:
                    location_timeout = insertdict['location_timeout'] 

                #测试
#                activateduration = 90
#                location_timeout = 30
#                insertdict['activateduration'] = activateduration
#                insertdict['location_timeout'] = location_timeout

                # logger.debug(insertdict)
                updateredisdict.update(insertdict)
                obj = self.collect_memberinfo.insert_one(insertdict)
                insertMemberObjectId = obj.inserted_id
                loglist = list()
                logmember = dict()
                logmember['user'] = user
                logmember['key'] = insertMemberObjectId.__str__()
                logmember['table'] = 'members'
                logmember['result'] = '1'
                logmember['type'] = 'add'
                logmember['time'] = datetime.datetime.now()
                loglist.append(logmember)
                self.col_operatelogs.insert_many(loglist)

            retbody['memberid'] = insertMemberObjectId.__str__()
            retdict['error_code'] = self.ERRORCODE_OK

            for key in updateredisdict:
                if key in '_id':
                    updateredisdict[key] = updateredisdict[key].__str__()
                elif isinstance(updateredisdict[key],datetime.datetime):
                    updateredisdict[key] = updateredisdict[key].__str__()
            updateredisdict['_id'] = insertMemberObjectId.__str__()
            memberinfokey = self.KEY_MEMBERINFO % (insertMemberObjectId.__str__())
            self._redis.hmset(memberinfokey,updateredisdict)
            logger.debug("updateredisdict = %r" % updateredisdict)

            #打上标记
            setkey = self.KEY_MEMBER_SUVSTATE % (insertMemberObjectId.__str__())
            self._redis.set(setkey,'1')
            self._redis.expire(setkey, activateduration)

            setkey = self.KEY_MEMBER_LOCSTATE % (insertMemberObjectId.__str__())
            self._redis.set(setkey,'1')
            self._redis.expire(setkey, location_timeout)

            setkey = self.KEY_MEMBER_HEALTHSTATE % (insertMemberObjectId.__str__())
            self._redis.set(setkey,'1')
            self._redis.expire(setkey, self.DEFAULT_HEALTH_TIMEOUT)


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_info(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_info action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']

#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#
#            tnikey = resultlist[0]
#            logger.debug(tnikey)
#            tni = self._redis.hgetall(tnikey)
            tni,errorcode = self.getMemberinfoByToken(tid)
            if tni is None:
                retdict['error_code'] = errorcode
                return
            for key in tni:
                if key in ['username', 'nickname']:
                    retbody[key] = urllib.quote(tni[key])
                elif key == '_id':
                    retbody['member_id'] = str(tni[key])
                elif key in ['password', 'createtime']:
                    continue
                elif key in ['account', 'userprofile','last_sms_time','last_callrecord_time','device']:
                    retbody[key] = eval(tni[key])
                    if key == 'device':
                        for keyimei in retbody[key]:
                            if retbody[key][keyimei].get('relationship') is None:
                                retbody[key][keyimei]['relationship'] = ''

                else:
                    retbody[key] = tni[key]


            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    
    def _proc_action_member_active(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }'''

        logger.debug(" into _proc_action_member_active action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('memberid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['memberid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            memberid = action_body['memberid']
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            activateduration = 0
            location_timeout = self.DEFAULT_LOCATION_TIMEOUT
            updateredisdict = dict()
            deviceid = ''
            if 'deviceid' in action_body:
                deviceid = action_body['deviceid']

            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return
            if deviceid != '':
                imei = int(deviceid)
                deviceinfo = self.collect_gearinfo.find_one({'deviceid':deviceid})
                if deviceinfo is None:
                    retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                    return

                # siminfo = self.collect_siminfo.find_one({'simnumber':simnumber})
                # if siminfo is None:
                #     retdict['error_code'] = self.ERRORCODE_SIMNUMBER_NOT_EXIST
                #     return

                if 'follow' in deviceinfo and deviceinfo['follow']!= '':
                    retdict['error_code'] = self.ERRORCODE_IMEI_HAS_OWNER
                    return

                # if 'deviceid' in siminfo and siminfo['deviceid'] != '':
                #     retdict['error_code'] = self.ERRORCODE_SIMNUMBER_ALREADY_USED
                #     return

                insertdict = dict()
                for key in action_body:
                    if key in ['user']:
                        user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                        continue
                    elif key in ['activateduration','location_timeout']:
                        insertdict[key] = action_body[key]
                    else:
                        insertdict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

                insertdict['createtime'] = datetime.datetime.now()
                insertdict['device_objid'] = deviceinfo['_id'].__str__()
                insertdict['activetime'] = datetime.datetime.now()
                # insertdict['pasiwode'] = pasiwode
                insertdict['alarm_enable'] = '1'
                insertdict['fence_list'] = dict()
                insertdict['state'] = self.MEMBER_STATE_SURVEILLANCE
                insertdict['alarminfo'] = self.ALARM_TYPE_NORMAL
                if 'activateduration' not in insertdict or insertdict['activateduration'] is None:
                    insertdict['activateduration'] = self.MONITOR_TIMEOUT
                    activateduration = self.MONITOR_TIMEOUT
                else:
                    activateduration = insertdict['activateduration'] 

                if 'location_timeout' not in insertdict or insertdict['location_timeout'] is None:
                    insertdict['location_timeout'] = self.DEFAULT_LOCATION_TIMEOUT
                    location_timeout = self.DEFAULT_LOCATION_TIMEOUT
                else:
                    location_timeout = insertdict['location_timeout'] 

                #测试
#                activateduration = 90
#                location_timeout = 30
#                insertdict['activateduration'] = activateduration
#                insertdict['location_timeout'] = location_timeout

                logger.debug(insertdict)
                updateredisdict.update(insertdict)
                obj = self.collect_memberinfo.insert_one(insertdict)
                insertMemberObjectId = obj.inserted_id

#                self.collect_gearinfo.update({'imei':imei},{'$set':{'follow':insertMemberObjectId.__str__()}})
                self.collect_gearinfo.update_one({'imei':imei},{'$set':{'follow':insertMemberObjectId.__str__(),'simnumber':simnumber,'activetime':datetime.datetime.now(),'state':self.AUTH_STATE_BIND,'username':username}})
#                followlist = list()
#                followlist.append(insertMemberObjectId.__str__())
                deviceinfo['follow'] = insertMemberObjectId.__str__()
                deviceinfo['simnumber'] = simnumber
                deviceinfo['state'] = self.AUTH_STATE_BIND
                deviceinfo['username'] = username
                deviceinfo['activetime'] = datetime.datetime.now().__str__()

#                gearmemberkey = self.KEY_IMEI_MNAME_MID % (deviceid,username,insertMemberObjectId.__str__())
#                self._redis.hmset(gearmemberkey,deviceinfo)
#                self._redis.expire(gearmemberkey,self.MONITOR_TIMEOUT)
                deviceinfokey = self.KEY_DEVICEINFO % (deviceid)
                self._redis.hmset(deviceinfokey,deviceinfo)
                self._redis.expire(deviceinfokey,self.MONITOR_TIMEOUT)

                setkey = self.KEY_DEVICESUVKEY % (deviceid)
                self._redis.set(setkey,'1')
                self._redis.expire(setkey,self.MONITOR_TIMEOUT)

                # self.collect_siminfo.update_one({'simnumber':simnumber},{'$set':{'deviceid':deviceid,'username':username,'device_objid':deviceinfo['_id'].__str__(),'memberid':insertMemberObjectId.__str__(),'state':self.SIM_STATE_BIND,'activetime':datetime.datetime.now()}})

                # #更新sim卡表
                # simupdatedict = dict()
                # simupdatedict['deviceid'] = deviceid
                # simupdatedict['memberid'] = insertMemberObjectId.__str__()
                # simupdatedict['username'] = username
                # simupdatedict['devicetype'] = action_body.get('devicetype')
                # simupdatedict['activatetime'] = datetime.datetime.now()
                # simupdatedict['state'] = self.SIM_STATE_BIND
                # self.collect_siminfo.update_one({'simnumber':simnumber},{'$set':simupdatedict})

                loglist = list()
                logmember = dict()
                logmember['user'] = user
                logmember['key'] = insertMemberObjectId.__str__()
                logmember['table'] = 'members'
                logmember['result'] = '1'
                logmember['type'] = 'add'
                logmember['time'] = datetime.datetime.now()
                loglist.append(logmember)

                loggear = dict()
                loggear['user'] = user
                loggear['key'] = deviceinfo['_id'].__str__()
                loggear['table'] = 'devices'
                loggear['result'] = '1'
                loggear['type'] = 'upate'
                loggear['time'] = datetime.datetime.now()
                loglist.append(loggear)

                # logsim = dict()
                # logsim['user'] = user
                # logsim['key'] = siminfo['_id'].__str__()
                # logsim['table'] = 'sims'
                # logsim['result'] = '1'
                # logsim['type'] = 'update'
                # logsim['time'] = datetime.datetime.now()
                # loglist.append(logsim)
                self.col_operatelogs.insert_many(loglist)
            else:
                insertdict = dict()
                for key in action_body:
                    if key in ['user']:
                        user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                        continue
                    elif key in ['activateduration','location_timeout']:
                        insertdict[key] = action_body[key]
                    else:
                        insertdict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

                insertdict['createtime'] = datetime.datetime.now()
                insertdict['device_objid'] = ''
                insertdict['activetime'] = datetime.datetime.now()
#                insertdict['activateduration'] = self.MONITOR_TIMEOUT
                # insertdict['pasiwode'] = pasiwode
                insertdict['state'] = self.MEMBER_STATE_SURVEILLANCE
                if 'activateduration' not in insertdict or insertdict['activateduration'] is None:
                    insertdict['activateduration'] = self.MONITOR_TIMEOUT
                    activateduration = self.MONITOR_TIMEOUT
                else:
                    activateduration = insertdict['activateduration'] 

                if 'location_timeout' not in insertdict or insertdict['location_timeout'] is None:
                    insertdict['location_timeout'] = self.DEFAULT_LOCATION_TIMEOUT
                    location_timeout = self.DEFAULT_LOCATION_TIMEOUT
                else:
                    location_timeout = insertdict['location_timeout'] 

                #测试
#                activateduration = 90
#                location_timeout = 30
#                insertdict['activateduration'] = activateduration
#                insertdict['location_timeout'] = location_timeout

                logger.debug(insertdict)
                updateredisdict.update(insertdict)
                obj = self.collect_memberinfo.insert_one(insertdict)
                insertMemberObjectId = obj.inserted_id

            retbody['memberid'] = insertMemberObjectId.__str__()
            retdict['error_code'] = self.ERRORCODE_OK

            for key in updateredisdict:
                if key in '_id':
                    updateredisdict[key] = updateredisdict[key].__str__()
                elif isinstance(updateredisdict[key],datetime.datetime):
                    updateredisdict[key] = updateredisdict[key].__str__()
            updateredisdict['_id'] = insertMemberObjectId.__str__()
            memberinfokey = self.KEY_MEMBERINFO % (insertMemberObjectId.__str__())
            self._redis.hmset(memberinfokey,updateredisdict)
            logger.debug("updateredisdict = %r" % updateredisdict)

            #打上标记
            setkey = self.KEY_MEMBER_SUVSTATE % (insertMemberObjectId.__str__())
            self._redis.set(setkey,'1')
            self._redis.expire(setkey, activateduration)

            setkey = self.KEY_MEMBER_LOCSTATE % (insertMemberObjectId.__str__())
            self._redis.set(setkey,'1')
            self._redis.expire(setkey, location_timeout)

            setkey = self.KEY_MEMBER_HEALTHSTATE % (insertMemberObjectId.__str__())
            self._redis.set(setkey,'1')
            self._redis.expire(setkey, self.DEFAULT_HEALTH_TIMEOUT)

            loglist = list()
            logmember = dict()
            # logmember['user'] = user
            logmember['key'] = insertMemberObjectId.__str__()
            logmember['table'] = 'members'
            logmember['result'] = '1'
            logmember['type'] = 'active'
            logmember['time'] = datetime.datetime.now()
            loglist.append(logmember)
            self.col_operatelogs.insert_many(loglist)

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_update(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                        email   O   
                        mobile  O   手机
                        nickname    O   nickname 用qutoe编码
                        ios_device_token    O   
                        enable_notify   O   0-no 1-yes
                        birthday        
                        lang
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_update action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']

            updatedict = dict()
            userprofiledict = dict()
            userprofiledict['ios_token'] = list()
            last_sms_time = None;
            last_callrecord_time = None

            for key in action_body:
                if key in ['enable_notify', 'lang']:
                    userprofiledict[key] = action_body[key]
                elif key in ['ios_token']:
                    userprofiledict['ios_token'].append(action_body[key])
                elif key in ['last_sms_time']:
                    last_sms_time = action_body[key]
                elif key in ['last_callrecord_time']:
                    last_callrecord_time = action_body[key]
                elif key in ['tid','vid']:
                    continue
                else:
                    updatedict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')

#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#
#            tnikey = resultlist[0]
#            logger.debug(tnikey)
#            tni = self._redis.hgetall(tnikey)
            tni,errorcode = self.getMemberinfoByToken(tid)
            if tni is None:
                retdict['error_code'] = errorcode
                return
            member_id = ObjectId(tni['_id'].__str__())
            tnikey = self.KEY_MEMBERINFO % (member_id.__str__())
            self._redis.hmset(tnikey, updatedict)

            if 'userprofile' in tni:
                old_userprofiledict = eval(tni['userprofile'])
                if 'ios_token' not in old_userprofiledict:
                   old_userprofiledict['ios_token'] = list()
            else:
                old_userprofiledict = dict()
                old_userprofiledict['ios_token'] = list()

            for key in userprofiledict:
                if key == 'ios_token':
                    old_userprofiledict['ios_token'] = list(set(old_userprofiledict['ios_token']).union(set(userprofiledict[key])))
                else:
                    old_userprofiledict[key] = userprofiledict[key]

            self._redis.hset(tnikey, 'userprofile', old_userprofiledict)

            if last_sms_time:
                tmplist = last_sms_time.split(';')
                if len(tmplist) == 2:
                    imei = tmplist[0]
                    timestamp = tmplist[1]
                    if 'last_sms_time' in tni:
                        lastdict = eval(tni.get('last_sms_time'))
                    else:
                        lastdict = dict()

                    lastdict[imei] = timestamp
                    self._redis.hset(tnikey,'last_sms_time', lastdict)
                    self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'last_sms_time':lastdict}})

            if last_callrecord_time:
                tmplist = last_callrecord_time.split(';')
                if len(tmplist) == 2:
                    imei = tmplist[0]
                    timestamp = tmplist[1]
                    if 'last_callrecord_time' in tni:
                        lastdict = eval(tni.get('last_callrecord_time'))
                    else:
                        lastdict = dict()

                    lastdict[imei] = timestamp
                    self._redis.hset(tnikey,'last_callrecord_time', lastdict)
                    self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'last_callrecord_time':lastdict}})


            if 'ios_token' in old_userprofiledict:
                old_userprofiledict.pop('ios_token')

            self.collect_memberinfo.update_one({'_id':member_id}, {'$set':updatedict})
            self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'userprofile':old_userprofiledict}})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


#    def _proc_action_change_pasiwode(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'change_pasiwode', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                    tid M   tokenid
#                    vid M   
#                    old_pasiwode    M   用户密码（verifycode = “abcdef”, 
#                                        先用m0 = md5(verifycode),
#                                        再m1 = md5(pasiwode+m0)
#
#                    new_pasiwode    M   用户密码（verifycode = “abcdef”, 
#                                        先用m0 = md5(verifycode),
#                                        再m1 = md5(pasiwode+m0)
#
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_change_pasiwode action_body:%s"%action_body)
#        try:
##        if 1:
#            
#            if ('tid' not in action_body) or  ('vid' not in action_body) or ('old_pasiwode' not in action_body) or  ('new_pasiwode' not in action_body):
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            if action_body['tid'] is None or action_body['vid'] is None or action_body['old_pasiwode'] is None or action_body['new_pasiwode'] is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            vid = action_body['tid']
#            tid = action_body['tid']
#            old_pasiwode = action_body['old_pasiwode']
#            new_pasiwode = action_body['new_pasiwode']
#
##            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
##            resultlist = self._redis.keys(searchkey)
##            logger.debug("resultlist = %r" % (resultlist))
##            if len(resultlist) == 0:
##                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
##                return
##
##            tnikey = resultlist[0]
##            logger.debug(tnikey)
##            tni = self._redis.hgetall(tnikey)
##            if tni is None:
##                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
##                return
##            member_id = ObjectId(tni['_id'])
#            tni,errorcode = self.getMemberinfoByToken(tid)
#            if tni is None:
#                retdict['error_code'] = errorcode
#                return
#            member_id = ObjectId(tni['_id'].__str__())
#            tnikey = self.KEY_MEMBERINFO % (member_id.__str__())
#            
#            if 'pasiwode' not in tni or tni['pasiwode'] != old_pasiwode:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_pasiwode_INVALID
#                return
#
#            self._redis.hset(tnikey, 'pasiwode', new_pasiwode)    
#            self.collect_memberinfo.update({'_id':member_id}, {'$set':{'pasiwode':new_pasiwode}})
#            retdict['error_code'] = self.ERRORCODE_OK
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
#
#
#
#
#
#    def _proc_action_getback_pasiwode(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'getback_pasiwode', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                        username    M   用户名
#                        vid M   
#                        lang    O   当前版本的语言
#                                    eng
#                                    chs
#                                    cht
#                                    …
#
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_getbackpasiwode action_body")
#        try:
##        if 1:
#            
#            if ('username' not in action_body) or  ('vid' not in action_body):
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            if action_body['username'] is None or action_body['vid'] is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            nationalcode = '86'
#            if 'nationalcode' in action_body and action_body['nationalcode'] is not None:
#                nationalcode = action_body['nationalcode']
#
#            lang = 'chs'
#            if 'lang' in action_body and action_body['lang'] is not None:
#                lang = action_body['lang']
#
#            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
#            vid = action_body['vid']
#
#            if nationalcode == '86' and username.startswith('86'):
#                username1 = username[2:]
#                memberinfo = self.collect_memberinfo.find_one({'username':{'$in':[username, username1]}})
#            else:
#                memberinfo = self.collect_memberinfo.find_one({'username':username})
#
#            if memberinfo is None:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
#                return
#
#            if 'email' not in memberinfo or memberinfo['email'] == "":
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NO_EMAIL
#                return
#
#            newpasiwode = "%d" % random.SystemRandom().randint(100000,999999)
#            md5pasiwode = self.calcpasiwode(newpasiwode, self._json_dbcfg.get("verifycode"))
#            logger.debug("newpasiwode = %s, md5pasiwode = %s" % (newpasiwode,md5pasiwode))
#            #找对应的语言模板
#            fileobj = open("/opt/Keeprapid/KRWatch/server/conf/notifytempelate.conf","r")
#            templateinfo = json.load(fileobj)
#            fileobj.close()
#
#            getbackpasiwode_t = templateinfo[self.NOTIFY_TYPE_GETBACK]
#            if lang not in getbackpasiwode_t:
#                templateinfo = getbackpasiwode_t['eng']
#            else:
#                templateinfo = getbackpasiwode_t[lang]
#
#            sendcontent = templateinfo['content'] % (newpasiwode)
#            body = dict()
#            body['dest'] = memberinfo['email']
#            body['content'] = urllib.quote(sendcontent.encode('utf-8'))
#            body['carrier'] = 'email'
#            body['notify_type'] = self.NOTIFY_TYPE_GETBACK
#            body['email_from'] = urllib.quote(templateinfo['email_from'].encode('utf-8'))
#            body['email_subject'] = urllib.quote(templateinfo['email_subject'].encode('utf-8'))
#            action = dict()
#            action['body'] = body
#            action['version'] = '1.0'
#            action['action_cmd'] = 'send_email_notify'
#            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
#            action['from'] = ''
#
##            sendcontent = templateinfo['content'] % (newpasiwode)
##            body = dict()
##            body['mobile'] = mobile
##            body['content'] = urllib.quote(sendcontent.encode('utf-8'))
##            action = dict()
##            action['body'] = body
##            action['version'] = '1.0'
##            action['action_cmd'] = 'send_sms'
##            action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
##            action['from'] = ''
#
#            self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))
#
#            #修改密码
#            self.collect_memberinfo.update({'_id':memberinfo['_id']},{'$set':{'pasiwode':md5pasiwode}})
#            #删除redis的cache数据
#            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username, memberinfo['_id'].__str__())
#            resultlist = self._redis.keys(searchkey)
#            if len(resultlist):
#                self.redisdelete(resultlist)
#
#            retdict['error_code'] = self.ERRORCODE_OK
#            return
#
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
#
#
#    def _proc_action_reset_pasiwode(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'getback_pasiwode', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                        username    M   用户名
#                        vid M   
#                        lang    O   当前版本的语言
#                                    eng
#                                    chs
#                                    cht
#                                    …
#
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_reset_pasiwode action_body")
#        try:
##        if 1:
#            
#            if ('username' not in action_body) or  ('vid' not in action_body) or ('pasiwode' not in action_body):
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            if action_body['username'] is None or action_body['vid'] is None or action_body['pasiwode'] is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            nationalcode = '86'
#            if 'nationalcode' in action_body and action_body['nationalcode'] is not None:
#                nationalcode = action_body['nationalcode']
#
#            lang = 'chs'
#            if 'lang' in action_body and action_body['lang'] is not None:
#                lang = action_body['lang']
#
#            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
#            vid = action_body['vid']
#            #app传递的pasiwode已经是md5加密过的数据
#            pasiwode = action_body['pasiwode']
#
#            if nationalcode == '86' and username.startswith('86'):
#                username1 = username[2:]
#                memberinfo = self.collect_memberinfo.find_one({'username':{'$in':[username, username1]}})
#            else:
#                memberinfo = self.collect_memberinfo.find_one({'username':username})
#
#            if memberinfo is None:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
#                return
#
#            #修改密码
#            self.collect_memberinfo.update({'_id':memberinfo['_id']},{'$set':{'pasiwode':pasiwode}})
#            #删除redis的cache数据
#            searchkey = self.KEY_TOKEN_NAME_ID % ('*',memberinfo['username'], memberinfo['_id'].__str__())
#            resultlist = self._redis.keys(searchkey)
#            if len(resultlist):
#                self.redisdelete(resultlist)
#
#            retdict['error_code'] = self.ERRORCODE_OK
#            return
#
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
#
#    def _proc_action_member_update_gear(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'member_update_gear', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                        tid     tokenid
#                        imei    M   用户id
#                        vid M   厂商id
#                        mobile  O   设备手机号
#                        name    O   用户名
#                        gender  O   用户性别
#                        height  O   cm
#                        weight  O   kg
#                        bloodtype   O   
#                        stride  O   
#                        birth   O   ‘2014-09-23’
#                        relationship    O   
#                        alarm_enable    O   
#                        alarm_center_lat    O   警戒围栏中心纬度
#                        alarm_center_long   O   警戒围栏中心经度
#                        alarm_center_radius O   警戒围栏半径（米）
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_member_update_gear action_body:%s"%action_body)
#        try:
##        if 1:
#            
#            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body):
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            vid = action_body['tid']
#            tid = action_body['tid']
#            imei_str = action_body['imei']
#            imei = int(imei_str)
#
#            updategeardict = dict()
#            relationship = None
#            for key in action_body:
#                if key in ['tid','vid','imei']:
#                    continue
#                elif key == 'nickname':
#                    updategeardict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
#                elif key == 'relationship':
#                    if action_body[key] is None:
#                        relationship = ""
#                    else:
#                        relationship = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
#                else:
#                    updategeardict[key] = action_body[key]
#
#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#
#            tnikey = resultlist[0]
#            logger.debug(tnikey)
#            tni = self._redis.hgetall(tnikey)
#            if tni is None:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#            object_member_id = ObjectId(tni['_id'])
#            memberid = tni['_id']
#
#            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
#            resultlist = self._redis.keys(searchkey)
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
#                return
#            imeikey = resultlist[0]
#            imeiinfo = self._redis.hgetall(imeikey)
#
#            #检查member是否有添加过imei
#            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
#                return
#
#            device = eval(tni['device'])
#            if imei_str not in device:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
#                return
#
#            #检查gear是否被member关注
#            if 'follow' not in imeiinfo or imeiinfo['follow'] is None or imeiinfo['follow'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
#                return
#
#            follow = eval(imeiinfo['follow'])
#            if memberid not in follow:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
#                return
#
#                
#            device[imei_str]['relationship'] = relationship
#            self._redis.hset(tnikey , 'device', device)
#            self.collect_memberinfo.update({'_id':object_member_id},{'$set':{'device':device}})
#
#            logger.debug(updategeardict)
#            for key in updategeardict:
#                self._redis.hset(imeikey, key, updategeardict[key])
#
#            if len(updategeardict):
#                self.collect_gearinfo.update({'imei':imei},{'$set':updategeardict})
#
#            retdict['error_code'] = self.ERRORCODE_OK
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
#
#
#    def _proc_action_member_del_gear(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'member_del_gear', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                        tid     tokenid
#                        imei    M   用户id
#                        vid M   厂商id
#
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_member_del_gear action_body:%s"%action_body)
#        try:
##        if 1:
#            
#            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body):
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            vid = action_body['tid']
#            tid = action_body['tid']
#            imei_str = action_body['imei']
#            imei = int(imei_str)
#
#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#
#            tnikey = resultlist[0]
#            logger.debug(tnikey)
#            tni = self._redis.hgetall(tnikey)
#            if tni is None:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#            object_member_id = ObjectId(tni['_id'])
#            memberid = tni['_id']
#
#            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
#            resultlist = self._redis.keys(searchkey)
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
#                return
#            imeikey = resultlist[0]
#            imeiinfo = self._redis.hgetall(imeikey)
#
#            #检查member是否有添加过imei
#            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
#                return
#            #检查gear是否被member关注
#            if 'follow' not in imeiinfo or imeiinfo['follow'] is None or imeiinfo['follow'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
#                return
#
#            device = eval(tni['device'])
#            if imei_str in device:
#                device.pop(imei_str)
#
#
#            follow = eval(imeiinfo['follow'])
#            if memberid in follow:
#                follow.remove(memberid)
#
#
#            self._redis.hset(tnikey , 'device', device)
#            self._redis.hset(imeikey, 'follow', follow)
#
#            self.collect_memberinfo.update({'_id':object_member_id},{'$set':{'device':device}})
#
#            self.collect_gearinfo.update({'imei':imei},{'$set':{'follow':follow}})
#
#            retdict['error_code'] = self.ERRORCODE_OK
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
#
#    def _proc_action_member_gear_headimg_upload(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'member_del_gear', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                        tid     tokenid
#                        imei    M   用户id
#                        vid M   厂商id
#
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_member_gear_headimg_upload action_body:%s"%action_body)
#        try:
##        if 1:
#            
#            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) or  ('img' not in action_body) or  ('format' not in #action_body):
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            if action_body['tid'] is None or action_body['vid'] is None or action_body['imei'] is None or action_body['format'] is None or action_body['img'] #is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            vid = action_body['vid']
#            tid = action_body['tid']
#            imei_str = action_body['imei']
#            imei = int(imei_str)
#
#            format = action_body['format']
#            img = action_body['img']
#
#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#
#            tnikey = resultlist[0]
#            logger.debug(tnikey)
#            tni = self._redis.hgetall(tnikey)
#            if tni is None:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#            object_member_id = ObjectId(tni['_id'])
#            memberid = tni['_id']
#
#            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
#            resultlist = self._redis.keys(searchkey)
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
#                return
#            imeikey = resultlist[0]
#            imeiinfo = self._redis.hgetall(imeikey)
#
#            #检查member是否有添加过imei
#            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
#                return
#            #检查gear是否被member关注
#            if 'follow' not in imeiinfo or imeiinfo['follow'] is None or imeiinfo['follow'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
#                return
#
#            #找到user，处理上传的头像
#            fileobj = open("/opt/Keeprapid/KRWatch/server/conf/upload.conf",'r')
#            uploadconfig = json.load(fileobj)
#            fileobj.close()
#
#            filedir = "%s%s" % (uploadconfig['FILEDIR_IMG_HEAD'], vid)
#            filename = "%s.%s" % (imei_str, format)
#            imgurl = "%s/%s/%s" % (uploadconfig['imageurl'],vid,filename)
#            logger.debug(filedir)
#            logger.debug(filename)
#            logger.debug(imgurl)
#
#            if os.path.isdir(filedir):
#                pass
#            else:
#                os.mkdir(filedir)
#
#            filename = "%s/%s" % (filedir, filename)
#            fs = open(filename, 'wb')
#            fs.write(base64.b64decode(img))
#            fs.flush()
#            fs.close()
#
#            retbody['img_url'] = imgurl
#
#            self._redis.hset(imeikey, 'img_url', imgurl)
#            self.collect_gearinfo.update({'imei':imei},{'$set':{'img_url':imgurl}})
#
#            retdict['error_code'] = self.ERRORCODE_OK
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
#
#    def _proc_action_member_gear_add_fence(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'member_update', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                        'tid'    : M
#                        'vid'     : M
#                        'email'   : O
#                        'mobile'  : O
#                        'nickname': O
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_member_gear_add_fence action_body:%s"%action_body)
#        try:
#            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('username' not in action_body) :
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if action_body['tid'] is None or action_body['tid'] == '':
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if action_body['vid'] is None or action_body['vid'] == '':
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if action_body['username'] is None or action_body['username'] == '':
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if ('fence_name' not in action_body) or action_body['fence_name'] is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if ('coord_list' not in action_body) or action_body['coord_list'] is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            fence_direction = self.FENCE_DIRECTION_OUT
#            if 'fence_direction' in action_body and action_body['fence_direction'] is not None:
#                fence_direction = action_body['fence_direction']
#
#            fence_enable = 1
#            if 'fence_enable' in action_body and action_body['fence_enable'] is not None:
#                fence_enable = int(action_body['fence_enable'])
#
#
#            vid = action_body['vid'];
#            tid = action_body['tid'];
#            username = action_body['username'];
#            user = ''
#            if 'user' in action_body:
#                user = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
#
#            memberinfo = self.collect_memberinfo.find_one({'username':username})
#            if memberinfo is None:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
#                return
#
#            fence_name = urllib.unquote(action_body['fence_name'].encode('utf-8')).decode('utf-8')
#            coord_list = action_body['coord_list']
#            logger.debug(coord_list)
#
#
#
#
#            if 'fence_list' in memberinfo:
#                oldfencelist = memberinfo['fence_list']
#            else:
#                oldfencelist = dict()
#
#            fenceinfo = dict()
#            oldfencelist[fence_name] = fenceinfo
#            fenceinfo['fence_direction'] = fence_direction
#            fenceinfo['fence_enable'] = fence_enable
#            fenceinfo['coord_list'] = coord_list
#
#
#            self.collect_memberinfo.update({'username':username},{'$set':{'fence_list':oldfencelist}})
#
#            logmember = dict()
#            logmember['user'] = user
#            logmember['key'] = memberinfo['_id'].__str__()
#            logmember['table'] = 'members'
#            logmember['result'] = '1'
#            logmember['type'] = 'update'
#            logmember['time'] = datetime.datetime.now()
#            self.col_operatelogs.insert(logmember)
#
#            retdict['error_code'] = self.ERRORCODE_OK
#            return
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
#
#    def _proc_action_member_update_fence_notify(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'member_update', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                        'tid'    : M
#                        'vid'     : M
#                        'email'   : O
#                        'mobile'  : O
#                        'nickname': O
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_member_update_fence_notify action_body:%s"%action_body)
#        try:
#            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('imei' not in action_body) :
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if action_body['tid'] is None or action_body['tid'] == '':
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if action_body['vid'] is None or action_body['vid'] == '':
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if action_body['imei'] is None or action_body['imei'] == '':
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if ('enable_fence' not in action_body) or action_body['enable_fence'] is None:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#
#            vid = action_body['vid'];
#            tid = action_body['tid'];
#            imei_str = action_body['imei'];
#            imei = int(action_body['imei'])
#            enable_fence = action_body['enable_fence']
#
#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#
#            tnikey = resultlist[0]
#            logger.debug(tnikey)
#            tni = self._redis.hgetall(tnikey)
#            if tni is None:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
#            object_member_id = ObjectId(tni['_id'])
#            memberid = tni['_id']
#
#
#            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
#                return
#
#            device = eval(tni['device'])
#            if imei_str not in device:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
#                return
#
#            imeiinfo = device[imei_str]
#            imeiinfo['enable_fence'] = enable_fence
#
#            self._redis.hset(tnikey,'device',device)
#            self.collect_memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'device':device}})
#            retdict['error_code'] = self.ERRORCODE_OK
#            return
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
#
#    def _proc_action_gear_reset(self, version, action_body, retdict, retbody):
#        '''
#        input : {    'action_cmd'  : 'member_update', M
#                     'seq_id      : M
#                     'version'    : M
#                     'body'   :{
#                        'tid'    : M
#                        'vid'     : M
#                        'email'   : O
#                        'mobile'  : O
#                        'nickname': O
#                    }
#                }
#
#        output:{   
#                   'error_code       : "200"'
#                   'seq_id'         : M
#                    body:{
#                   }
#                }
#        '''
#        logger.debug(" into _proc_action_gear_reset action_body:%s"%action_body)
#        try:
#            if 'imeilist' not in action_body:
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#            if action_body['imeilist'] is None or action_body['imeilist'] == '':
#                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
#                return
#
#            imeilist = action_body['imeilist'].split(',');
#
#            for imei in imeilist:
#                imei_int = int(imei)
#                resetdict = dict()
#                resetdict['last_long'] = 0
#                resetdict['alert_motor'] = 0
#                resetdict['last_lat'] = 0
#                resetdict['last_location_time'] = ''
#                resetdict['height'] = '0'
#                resetdict['follow'] = list()
#                resetdict['alarm_lat'] = 0
#                resetdict['weight'] = '0'
#                resetdict['nickname'] = ''
#                resetdict['alert_ring'] = '0'
#                resetdict['fence_list'] = dict()
#                resetdict['last_location_objectid'] = ''
#                resetdict['last_location_type'] = '0'
#                resetdict['img_url'] = ''
#                resetdict['friend_number'] = ''
#                resetdict['battery_level'] = '0'
#                resetdict['last_angle'] = '0'
#                resetdict['alarm_long'] = 0
#                resetdict['monitor_number'] = ''
#                resetdict['last_vt'] = '0'
#                resetdict['birth'] = ''
#                resetdict['last_angel'] = 0
#                resetdict['sos_number'] = ''
#                resetdict['alarm_radius'] = 0
#                resetdict['last_at'] = '0'
#                resetdict['mobile'] = ''
#                resetdict['last_lbs_objectid'] = ''
#                resetdict['alarm_enable'] = 1
#                resetdict['owner'] = ''
#
#
#                gearinfo = self.collect_gearinfo.find_one({'imei':imei_int})
#                if gearinfo != None:
#                    follow = gearinfo['follow']
#                    for memberid in follow:
#                        memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
#                        deviceinfo = memberinfo['device']
#                        if imei in deviceinfo:
#                            deviceinfo.pop(imei)
#
#                        self.collect_memberinfo.update({'_id':ObjectId(memberid)},{'$set':{'device':deviceinfo}})
#                        membersearchkey = self.KEY_TOKEN_NAME_ID % ('*','*',memberid)
#                        result = self._redis.keys(membersearchkey)
#                        if len(result) > 0:
#                            memberkey = result[0]
#                            self._redis.hset(memberkey, 'device', deviceinfo)
#                self.collect_gearinfo.update({'imei':imei_int},{'$set':resetdict})
#                searchkey = self.KEY_IMEI_ID % (imei, '*')
#                result = self._redis.keys(searchkey)
#                if len(result) >0:
#                    gearkey = result[0]
#                    self._redis.hmset(gearkey, resetdict)
#
#                self.colgps.remove({'imei':imei_int})
#                self.collbs.remove({'imei':imei_int})
#                #清除数据
#                senddict = dict()
#                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], imei)
#                senddict['sendbuf'] = "generalcmd,SOS,,,,MONITOR,,,FRIEND,,,,,,#"
#
#                #先缓存消息到redis，等待回应或者等待下次发送
#                cmdkey = self.KEY_IMEI_CMD % (imei, self.CMD_TYPE_SET_NUMBER)
#                self._redis.hmset(cmdkey, senddict)
#                self._redis.hset(cmdkey, 'soslist', ',,')
#                self._redis.hset(cmdkey, 'monitorlist', ',')
#                self._redis.hset(cmdkey, 'friendlist', ',,,,')
#                self._redis.hset(cmdkey,'tid','')
#                self._redis.expire(cmdkey,self.UNSEND_MSG_TIMEOUT)
#
#                #发送消息
#                if 'mqtt_publish' in self._config:
#                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))
#
#            retdict['error_code'] = self.ERRORCODE_OK
#            return
#
#        except Exception as e:
#            logger.error("%s except raised : %s " % (e.__class__, e.args))
#            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_declare_req(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_declare_req action_body:%s"%action_body)
        try:
            if 'tid' not in action_body:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            tid = action_body['tid']
#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
            tni,errorcode = self.getMemberinfoByToken(tid)
            if tni is None:
                retdict['error_code'] = errorcode
                return
#            member_id = ObjectId(tni['_id'].__str__())
#            tnikey = self.KEY_MEMBERINFO % (member_id.__str__())

            default_contentkey = 'info_eng'
            language = 'eng'
            if 'language' in action_body:
                language = action_body['language']
        #查找所有的通知
            infolist = self.collect_infotemp.find({'state':self.DECLARE_STATE_ACTIVE})
            returnlist = list()
            for infotemp in infolist:
                logger.debug(infotemp)
                content = ""
                contentkey = 'info_%s' % (language)
                if contentkey in infotemp:
                    content = infotemp[contentkey]
                else:
                    content = infotemp.get(default_contentkey)
                    
                infodict = dict()
                infodict['content'] = urllib.quote(content.encode('utf-8'))
                infodict['info_id'] = infotemp['info_id']
                returnlist.append(infodict)

            retbody['infolist'] = returnlist
            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_declare(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_declare action_body:%s"%action_body)
        try:
            if 'tid' not in action_body or 'info_id' not in action_body:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '' or action_body['info_id'] is None or action_body['info_id'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            tid = action_body['tid']
            heartrate = 0
            if 'heartrate' in action_body:
                heartrate = action_body['heartrate']

#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
            #查找所有的通知
            tni,errorcode = self.getMemberinfoByToken(tid)
            if tni is None:
                retdict['error_code'] = errorcode
                return
            member_id = ObjectId(tni['_id'].__str__())
            tnikey = self.KEY_MEMBERINFO % (member_id.__str__())

#            tnikey = resultlist[0]
#            tnilist = tnikey.split(':')
#            username = tnilist[3]
#            memberid = tnilist[4]
            memberid = tni['_id'].__str__()
            username = tni['username']
            info_id = action_body['info_id']
            declareinfo = self.collect_infotemp.find_one({'info_id':info_id})
            if declareinfo is None:
                retdict['error_code'] = self.ERRORCODE_NO_DECLARE_INFO
                return
            declarestate = declareinfo.get('state')
            maxheartrate = 120
            if 'maxheartrate' in declareinfo:
                maxheartrate = declareinfo['maxheartrate']
            minheartrate = 45
            if 'minheartrate' in declareinfo:
                minheartrate = declareinfo['minheartrate']

            if declarestate is None or declarestate != self.DECLARE_STATE_ACTIVE:
                retdict['error_code'] = self.ERRORCODE_DECLARE_NOT_ACTIVE
                return                
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            stateinfo = self.collect_infostate.find_one({'memberid':memberid})
            needkey = ['codetype','username','simnumber','nation','devicetype','passland','deviceid','nickname','illgroup','nickname']
            memberinsertdict = dict()
            for key in needkey:
                memberinsertdict[key] = memberinfo.get(key)

            insertdict = dict()
            insertdict['username'] = username
            insertdict['memberid'] = memberid
            insertdict['info_id'] = info_id
            insertdict['info_desc'] = declareinfo.get('info_desc')
            insertdict['timestamp'] = datetime.datetime.now()
            insertdict['heartrate'] = heartrate
            insertdict.update(memberinsertdict)

            if heartrate > 0:
                datestr = time.strftime('%Y-%m-%d',time.localtime(time.time()))
                insertdata = dict()
                insertdata['member_id'] = memberid
                insertdata['username'] = memberinfo.get("username")
                insertdata['date_str'] = datestr
                insertdata['data_type'] = 'heart'
                insertdata['value'] = heartrate
                insertdata['value2'] = 0
                insertdata['imei'] = 0
                insertdata['deviceid'] = ''
                insertdata['datetime'] = datetime.datetime.now()
                insertdata['timestamp'] = int(time.time())
                self.collect_bodyfunction.insert_one(insertdata)

            if stateinfo is None:
                self.collect_infostate.insert_one(insertdict)
            else:
                self.collect_infostate.update_one({'memberid':memberid},{'$set':{'info_id':info_id,'info_desc':declareinfo.get('info_desc'),'timestamp':datetime.datetime.now(),'heartrate':heartrate}})

            self.collect_infolog.insert_one(insertdict)

            updatedict = dict()
            updatedict['last_health'] = info_id
            updatedict['last_health_time'] = datetime.datetime.now()
            updatedict['last_heartrate'] = heartrate

            self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':updatedict})


            healthkey = self.KEY_MEMBER_HEALTHSTATE % (memberid)
            if self._redis.exists(healthkey) is False:
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
            self._redis.set(healthkey,'1')
            self._redis.expire(healthkey,self.DEFAULT_HEALTH_TIMEOUT)

            isalarm = declareinfo.get('alarm')
            if isalarm is not None and isalarm == 1:
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
                if heartrate > 0:
                    if heartrate>maxheartrate or heartrate < minheartrate:
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



            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_gps_upload(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                        'email'   : O
                        'mobile'  : O
                        'nickname': O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_gps_upload action_body:%s"%action_body)
        try:
            if 'tid' not in action_body:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            tid = action_body['tid']
#            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
#            resultlist = self._redis.keys(searchkey)
#            if len(resultlist) == 0:
#                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
#                return
            tni,errorcode = self.getMemberinfoByToken(tid)
            if tni is None:
                retdict['error_code'] = errorcode
                return
            member_id = ObjectId(tni['_id'].__str__())
            tnikey = self.KEY_MEMBERINFO % (member_id.__str__())
        #查找所有的通知
            latitude = '0'
            if 'latitude' in action_body and action_body['latitude'] != '':
                latitude = action_body['latitude']
            longitude = '0'
            if 'longitude' in action_body and action_body['longitude'] != '':
                longitude = action_body['longitude']
            vt = '0'
            if 'vt' in action_body and action_body['vt'] != '':
                vt = action_body['vt']
            angle = '0'
            if 'angle' in action_body and action_body['angle'] != '':
                angle = action_body['angle']
            altitude = '0'
            if 'altitude' in action_body and action_body['altitude'] != '':
                altitude = action_body['altitude']

            country = ''
            if 'country' in action_body and action_body['country'] != '':
                country = action_body['country']
            province = ''
            if 'province' in action_body and action_body['province'] != '':
                province = action_body['province']
            city = ''
            if 'city' in action_body and action_body['city'] != '':
                city = action_body['city']
            citycode = ''
            if 'citycode' in action_body and action_body['citycode'] != '':
                citycode = action_body['citycode']
            district = ''
            if 'district' in action_body and action_body['district'] != '':
                district = action_body['district']
            adcode = ''
            if 'adcode' in action_body and action_body['adcode'] != '':
                adcode = action_body['adcode']
            township = ''
            if 'township' in action_body and action_body['township'] != '':
                township = action_body['township']
            towncode = ''
            if 'towncode' in action_body and action_body['towncode'] != '':
                towncode = action_body['towncode']
            address = ''
            if 'address' in action_body and action_body['address'] != '':
                address = action_body['address']


#            tnikey = resultlist[0]
            memberid = tni['_id'].__str__()
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return
            if memberinfo['state'] != self.MEMBER_STATE_SURVEILLANCE:
                logger.debug("Member out of Surveillance")
                retdict['error_code'] = self.ERRORCODE_OK
                return
            locationtimeout = memberinfo.get('location_timeout')
            if locationtimeout is None:
                locationtimeout = self.DEFAULT_LOCATION_TIMEOUT
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
        
            insertdict = dict()
            insertdict['imei'] = ''
            insertdict['battery_level'] = '0'
            insertdict['longitude'] = longitude
            insertdict['latitude'] = latitude
            insertdict['altitude'] = altitude
            insertdict['vt'] = vt
            insertdict['location_type'] = self.LOCATION_TYPE_PHONE
            insertdict['timestamp'] = datetime.datetime.now()
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

            insertdict['country'] = country
            insertdict['province'] = province
            insertdict['city'] = city
            insertdict['citycode'] = citycode
            insertdict['district'] = district
            insertdict['adcode'] = adcode
            insertdict['township'] = township
            insertdict['towncode'] = towncode
            insertdict['address'] = address

            obj = self.colgps.insert_one(insertdict)
            insertobj = obj.inserted_id

            #更新到gearinfo中去
            if insertobj:
                updatedict = dict()
                updatedict['last_long'] = longitude
                updatedict['last_lat'] = latitude
                updatedict['last_at'] = altitude
                updatedict['last_vt'] = vt
                updatedict['last_angle'] = angle
                updatedict['last_location_objectid'] = insertobj.__str__()
                updatedict['last_location_type'] = self.LOCATION_TYPE_PHONE
                updatedict['last_location_time'] = datetime.datetime.now()
                updatedict['country'] = country
                updatedict['province'] = province
                updatedict['city'] = city
                updatedict['citycode'] = citycode
                updatedict['district'] = district
                updatedict['adcode'] = adcode
                updatedict['township'] = township
                updatedict['towncode'] = towncode
                updatedict['address'] = address


                self.collect_memberinfo.update_one({'_id':memberinfo['_id']},{'$set':updatedict})
            #查看是否需要围栏服务
            if 'alarm_enable' in memberinfo and memberinfo['alarm_enable'] == '1':
                body = dict()
                body['latitude'] = latitude
                body['longitude'] = longitude
                body['memberid'] = memberinfo['_id'].__str__()
                action = dict()
                action['body'] = body
                action['version'] = '1.0'
                action['action_cmd'] = 'check_location'
                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                action['from'] = ''
                if 'session' in self._config:
                    self._sendMessage(self._config['session']['Consumer_Queue_Name'], json.dumps(action))
            #取逆向地理信息,
            if country == '':
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
    if _config is not None and 'member' in _config and _config['member'] is not None:
        if 'thread_count' in _config['member'] and _config['member']['thread_count'] is not None:
            thread_count = int(_config['member']['thread_count'])

    for i in xrange(0, thread_count):
        memberlogic = MemberLogic(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)
