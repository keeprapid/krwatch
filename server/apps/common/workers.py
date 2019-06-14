#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  asset_main.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo wokers类的基类

import json
import logging
#import time
import struct
import logging.config
logging.config.fileConfig("/opt/Keeprapid/KRWatch/server/conf/log.conf")
logger = logging.getLogger('krwatch')



class WorkerBase():

    ERROR_RSP_UNKOWN_COMMAND = '{"seq_id":"123456","body":{},"error_code":"40000"}'
    FILEDIR_IMG_HEAD = '/mnt/www/html/krwatch/image/'
    #errorcode define
    ERRORCODE_OK = "200"
    ERRORCODE_UNKOWN_CMD = "40000"
    ERRORCODE_SERVER_ABNORMAL = "40001"
    ERRORCODE_CMD_HAS_INVALID_PARAM = '40002'
    ERRORCODE_DB_ERROR = '40003'
    ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST = '41001'
    ERRORCODE_MEMBER_M_INVALID = '41002'
    ERRORCODE_MEMBER_NOT_EXIST = '41003'
    ERRORCODE_MEMBER_TOKEN_OOS = '41004'
    ERRORCODE_MEMBER_USER_ALREADY_EXIST = '41005'
    ERRORCODE_MEMBER_USERNAME_INVALID = '41006'
    ERRORCODE_MEMBER_SOURCE_INVALID = '41007'
    ERRORCODE_MEMBER_ALREADY_FOLLOW_GEAR = '41008'
    ERRORCODE_MEMBER_NOT_FOLLOW_GEAR = '41009'
    ERRORCODE_MEMBER_VERIFYCODE_ALREADY_INUSE = '41010'
    ERRORCODE_MEMBER_VERIFYCODE_FAILEDBY_GATEWAY = '41011'
    ERRORCODE_MEMBER_VERIFYCODE_ERROR = '41012'
    ERRORCODE_MEMBER_NOT_OWNER = '41013'
    ERRORCODE_MEMBER_NO_EMAIL = '41014'
    ERRORCODE_NO_DECLARE_INFO = '41015'
    ERRORCODE_DECLARE_NOT_ACTIVE = '41016'
    ERRORCODE_MEMBER_ID_INVALID = '41017'

    ERRORCODE_IMEI_NOT_EXIST = '42001'
    ERRORCODE_IMEI_OUT_MAXCOUNT = '42002'
    ERRORCODE_IMEI_HAS_OWNER = '42003'
    ERRORCODE_IMEI_STATE_OOS = '42004'
    ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW = '42005'
    ERRORCODE_IMEI_CACHE_ERROR = '42006'
    ERRORCODE_IMEI_HAS_SAME_CMD_NOT_RESPONE = '42007'
    ERRORCODE_IMEI_HAS_NO_FENCE = '42008'
    ERRORCODE_IMEI_FENCE_DEACTIVE = '42009'
    ERRORCODE_IMEI_HAS_NO_NUMBER = '42010'


    ERRORCODE_LBS_FAIL = '43001'

    ERRORCODE_SIMNUMBER_NOT_EXIST = '44001'
    ERRORCODE_SIMNUMBER_ALREADY_USED = '44002'


    MAX_LOCATION_NUMBER = 5
    #member_state
    MEMBER_STATE_INIT_ACTIVE = 0
    MEMBER_STATE_EMAIL_COMFIRM = 1

    # MEMBER_PASSWORD_VERIFY_CODE = "abcdef"
    #gear
    AUTH_STATE_NEW = 1
    AUTH_STATE_BIND = 2
    AUTH_STATE_OOS = 3
    AUTH_STATE_SCRAPPED = 4

    #memberstate
    MEMBER_STATE_UNSURVEILLANCE = 0
    MEMBER_STATE_SURVEILLANCE = 1
    MEMBER_STATE_SURVEILLANCE_FINISH = 2

    MEMBER_ALARM_STATE_OFF = 0
    MEMBER_ALARM_STATE_ON  = 1

    ALARM_STATE_UNREAD = 0
    ALARM_STATE_READ = 1
    #simcard
    SIM_STATE_NEW = 1
    SIM_STATE_BIND = 2
    SIM_STATE_OOS = 3

    GEAR_ONLINE_TIMEOUT = 30*60
    CLIENT_ONLINE_TIMEOUT = 600
    UNSEND_MSG_TIMEOUT = 30

    GEAR_MAX_FOLLOW_COUNT = 6

    CMD_TYPE_SET_NUMBER = 'SetSosNumber'
    CMD_TYPE_SET_CLOCK = 'SetClock'
    CMD_TYPE_START_LOCATION = 'StartLocation'
    
    #notify type
    NOTIFY_TYPE_GETBACK = 'GetBackPassword'
    NOTIFY_TYPE_SENDVC = 'SendVerifyCode'
    NOTIFY_TYPE_WARNNING = "Warnning"
    #redis
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
    KEY_IMEI_SOCKID = "K:ds:%s"
    KEY_IMEI_TIMESTAMP = "K:itm:%s"
    KEY_IMEI_SESSION_TIMEOUT = "K:session:%s"
    KEY_TRIGGER_IMEI_NOTIFY_TIMEOUT = "K:trigger:%s:%s"

    #email
    SEND_EMAIL_INTERVAL = 2

    #special
    ADMIN_VID = "888888888888"

    #location
    LOCATION_TYPE_GPS = '1'
    LOCATION_TYPE_LBS = '2'
    LOCATION_TYPE_PHONE = '3'
    
    MSG_TYPE_SMS = 'SMS'

    FENCE_DIRECTION_IN = 'in'
    FENCE_DIRECTION_OUT= 'out'
    CALL_DIRECTION_WATCH_OUTGOING = 1
    CALL_DIRECTION_WATCH_INCOMING_NORAML = 2
    CALL_DIRECTION_WATCH_INCOMING_MONITOR = 3
    
    CALL_ANSWERED = 1
    CALL_UNCONNECT = 2

    BATTERY_LEVEL_ALERT = 10
    LBS_MAX_DIFF_COUNT = 3

    NOTIFY_CODE_SET_SOS_OK = '10000'
    NOTIFY_CODE_SET_CLOCK_OK = '10001'
    NOTIFY_CODE_LOW_BATTERY = '10002'
    NOTIFY_CODE_NEW_SMS = '10003'
    NOTIFY_CODE_SOS_UNCONNECT = '10004'
    NOTIFY_CODE_LOCATION_OK = '20000'
    NOTIFY_CODE_LEAVE_FENCE = '20001'
    NOTIFY_CODE_INTO_FENCE = '20002'

    LBS_SERVICE_URL = "http://api.haoservice.com/api/viplbs"
    LBS_SERVICE_KEY = "05367aa46ebc40d2ae8e8ca9d082147c"
    

    LBS_SERVICE_AMAP_URL = "http://apilocate.amap.com/position?"
    LBS_SERVICE_AMAP_KEY = "d3e93f653350a0340c3bb9a57f78a4a0"

    GEO_SERVICE_AMAP_URL = "http://restapi.amap.com/v3/geocode/regeo?"
    GEO_SERVICE_AMAP_KEY = "6ee0a725136855fcd20b6b07826ece70"
    
    LBS_Coordinate_GPS = 2
    LBS_Coordinate_BAIDU = 1
    LBS_Coordinate_Google = 0


    VID_ODA = '00000E004002'
    VID_WISHONEY = '00000E004001'

    PROTOCOL_VERSION_2 = '2.0'
    PROTOCOL_VERSION_1 = '1.0'

    MONITOR_TIMEOUT = 31*24*60*60

    ALARM_TYPE_NORMAL = "normal"
    ALARM_TYPE_FENCE = "fencealarm"
    ALARM_TYPE_HEALTH = "healthcondition"
    ALARM_TYPE_TIME = "locationtimeout"
    ALARM_TYPE_HEALTHTIMEOUT = "healthtimeout"
    # ALARM_TYPE_HEALTH = 'heartrateabnormal'

    DEFAULT_LOCATION_TIMEOUT = 24*60*60
#    DEFAULT_LOCATION_TIMEOUT = 30

    DEFAULT_HEALTH_TIMEOUT = 24*60*60
    #DEFAULT_HEALTH_TIMEOUT = 30

    DEFAULT_FILTER_TIMEOUT = 3*60*60
    #DEFAULT_FILTER_TIMEOUT = 60

    DEFAULT_SESSION_CHECK_TIMEOUT = 3600

    DECLARE_STATE_ACTIVE = '1'
    DECLARE_STATE_OOS = '0'

    WATCH_CMD_REGISTER = 0x0008
    WATCH_CMD_REGISTER_ACK = 0x8008
    WATCH_CMD_LOGIN = 0x0001
    WATCH_CMD_LOGIN_ACK = 0x8001
    WATCH_CMD_CONFIG_TRAP = 0x0007
    WATCH_CMD_CONFIG_TRAP_ACK = 0x8007
    
    WATCH_TAG_STRING = 0xE001
    WATCH_TAG_DATETIME = 0xE002
    WATCH_TAG_INT = 0xE003
    WATCH_TAG_SHORT = 0xE004
    WATCH_TAG_BYTE = 0xE005
    WATCH_TAG_EVENT = 0xE006
    WATCH_TAG_AES = 0xE020
    WATCH_TAG_SUM = 0xE021

    WATCH_K = "kr123456"
    WATCH_AES_IV = "0102030405678912"
    WATCH_AES_KEY = "abcdef1234567890"

    WATCH_RESULT_OK = 0x0
    WATCH_REGISTER_RESULT_ERR_SAME_IMEI = 0x1
    WATCH_REGISTER_RESULT_ERR_NO_IMEI = 0x2
    WATCH_REGISTER_RESULT_ERR_SEQID = 0x3

    WATCH_SUB_CMD_SYNC_TIME = 0x7001
    WATCH_SUB_CMD_UPLOAD_ALARM = 0x700B
    WATCH_SUB_CMD_UPLOAD_LOCATION = 0x700C
    WATCH_SUB_CMD_UPLOAD_MANUAL_LOCATION = 0x700D
    WATCH_SUB_CMD_UPLOAD_STEP = 0x7015
    WATCH_SUB_CMD_UPLOAD_HEART = 0x7017
    WATCH_SUB_CMD_REQ_HEALTH = 0x7019
    WATCH_SUB_CMD_DECLARE_HEALTH = 0x701A
    WATCH_SUB_CMD_REQ_WEATHER = 0x7016
    WATCH_SUB_CMD_EVENT = 0x7010

    WATCH_EVENT_NOTIFY = 0x1A

    WATCH_EVENT_NOTIFY_MAXLEN = 210

    NOTIFY_TRIGGER_TYPE_SURVCLOSE = "CloseSurveillance"
    NOTIFY_TRIGGER_TYPE_HEALTHTIMEOUT = "HealthTimeout"

  
    def __init__(self):
        logger.debug("WorkerBase:__init__")
 
    def redisdelete(self, argslist):
        logger.debug('%s' % ('","'.join(argslist)))
        ret = eval('self._redis.delete("%s")'%('","'.join(argslist)))
        logger.debug('delete ret = %d' % (ret))

    def _sendMessage(self, to, body):
        #发送消息到routkey，没有返回reply_to,单向消息
#        logger.debug(to +':'+body)
        if to is None or to == '' or body is None or body == '':
            return

        self._redis.lpush(to, body)

    def getMemberinfoByToken(self,tid):
        tokenkey = self.KEY_TOKEN % (tid)
        if self._redis.exists(tokenkey) is False:
            return None,self.ERRORCODE_MEMBER_TOKEN_OOS
        memberid = self._redis.hget(tokenkey,'memberid')
        if memberid is None:
            self._redis.delete(tokenkey)
            return None,self.ERRORCODE_MEMBER_TOKEN_OOS
        memberinfokey = self.KEY_MEMBERINFO % (memberid)
        memberinfo = self._redis.hgetall(memberinfokey)
        if memberinfo:
            return memberinfo,self.ERRORCODE_OK
        else:
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo:
                updatedict = dict()
                updatedict.update(memberinfo)
                updatedict['token'] = tid
                for key in updatedict:
                    if isinstance(updatedict[key],datetime.datetime):
                        updatedict[key] = updatedict[key].__str__()
                self._redis.hmset(memberinfokey,updatedict)
                return memberinfo,self.ERRORCODE_OK
            else:
                return None,self.ERRORCODE_MEMBER_NOT_EXIST

    def getValuebyStruct(self,structstr,buf,buflen):
        if len(buf) < buflen:
            return None
        s = struct.Struct(structstr)
        if 'B' in structstr:
            return s.unpack(buf)
        else:
            return s.unpack(buf)[0]

