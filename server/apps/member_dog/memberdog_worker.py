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
import threading
import json
import logging
import logging.config
import redis
import random
import pymongo
from bson.objectid import ObjectId

if '/opt/Keeprapid/KRWatch/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/KRWatch/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/KRWatch/server/conf/log.conf")
logger = logging.getLogger('krwatch')


class MemberDog(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(MemberDog, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("MemberDog :running in __init__")

        fileobj = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:MemberDog"
        if 'member_dog' in _config:
            if 'Consumer_Queue_Name' in _config['member_dog']:
                self.recv_queue_name = _config['member_dog']['Consumer_Queue_Name']

        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])

        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.db = self.mongoconn.zhijian_member
        self.collect_memberinfo = self.db.members

    def run(self):
        logger.debug("Start MemberDog pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
        while 1:
            try:
#        if 1:
                t1 = time.time()
                now = time.mktime(datetime.datetime.now().timetuple())
                memberlist = self.collect_memberinfo.find({'state':self.MEMBER_STATE_SURVEILLANCE})
                for memberinfo in memberlist:
#                    logger.debug(memberinfo)
                    memberid = memberinfo['_id'].__str__()
                    suvstatekey = self.KEY_MEMBER_SUVSTATE % (memberid)
                    locationkey = self.KEY_MEMBER_LOCSTATE % (memberid)
                    healthkey = self.KEY_MEMBER_HEALTHSTATE % (memberid)
                    #检查监控标志位，失效了就修改监控状态
                    if self._redis.exists(suvstatekey) is False:
                        logger.debug("member[%s] out of time, change sate" % (memberinfo['username']))
                        self.collect_memberinfo.update_one({'_id':memberinfo['_id']},{'$set':{'state':self.MEMBER_STATE_SURVEILLANCE_FINISH}})
                        deviceid = memberinfo.get('deviceid')
                        if deviceid and deviceid!= '':                            
                            devicesuvkey = self.KEY_DEVICESUVKEY % (deviceid)
                            self._redis.delete(devicesuvkey)
                    else:
                        #在监控状态下再检查健康，定位等报警
                        if self._redis.exists(locationkey) is False:
                            filterkey = self.KEY_MEMBERLOCATIONALARMFILTER%(memberid)
                            if self._redis.exists(filterkey) is False:
                                self._redis.set(filterkey,'1')
                                self._redis.expire(filterkey,self.DEFAULT_FILTER_TIMEOUT)
                                body = dict()
                                body['username'] = memberinfo['username']
                                body['memberid'] = memberid
                                body['type'] = self.ALARM_TYPE_TIME
                                body['value'] = 0
                                body['latitude'] = 0
                                body['longitude'] = 0
                                last_time = memberinfo.get('last_location_time')
                                if last_time is not None:
                                    body['last_location_time'] = last_time.strftime('%Y-%m-%d %H:%M:%S')
                                    body['value'] = (datetime.datetime.now()-last_time).seconds
                                action = dict()
                                action['body'] = body
                                action['version'] = '1.0'
                                action['action_cmd'] = 'alarm_create'
                                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                                action['from'] = ''
                                self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))

                        if self._redis.exists(healthkey) is False:
                            filterkey = self.KEY_MEMBERHEALTHALARMFILTER%(memberid)
                            if self._redis.exists(filterkey) is False:
                                self._redis.set(filterkey,'1')
                                self._redis.expire(filterkey,self.DEFAULT_FILTER_TIMEOUT)
                                body = dict()
                                body['username'] = memberinfo['username']
                                body['memberid'] = memberid
                                body['type'] = self.ALARM_TYPE_HEALTHTIMEOUT
                                body['value'] = 0
                                body['latitude'] = 0
                                body['longitude'] = 0
                                last_time = memberinfo.get('last_health_time')
                                if last_time is not None:
                                    body['last_health_time'] = last_time.strftime('%Y-%m-%d %H:%M:%S')
                                    body['value'] = (datetime.datetime.now()-last_time).seconds
                                # if 'last_health_time' in memberinfo:
                                #     body['last_health_time'] = memberinfo['last_health_time'].strftime('%Y-%m-%d %H:%M:%S')
                                action = dict()
                                action['body'] = body
                                action['version'] = '1.0'
                                action['action_cmd'] = 'alarm_create'
                                action['seq_id'] = '%d' % random.SystemRandom().randint(0,10000)
                                action['from'] = ''
                                self._sendMessage(self._config['alarmcenter']['Consumer_Queue_Name'], json.dumps(action))
                logger.debug('MemberDog done in %f',time.time()-t1)
                time.sleep(30)
            except Exception as e:
                logger.debug("%s except raised : %s " % (e.__class__, e.args))



if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'member_dog' in _config and _config['member_dog'] is not None:
        if 'thread_count' in _config['member_dog'] and _config['member_dog']['thread_count'] is not None:
            thread_count = int(_config['member_dog']['thread_count'])

    for i in xrange(0, thread_count):
        process = MemberDog(i)
        process.setDaemon(True)
        process.start()

    while 1:
        time.sleep(1)
