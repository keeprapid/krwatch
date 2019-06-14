#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
# import gevent
# from gevent import monkey
# from gevent.server import StreamServer
# monkey.patch_socket()
from multiprocessing import Process
# from gevent import socket
import threading
import SocketServer
import redis
import sys
import time
import uuid
import hashlib
import json
# import threading
import os
import logging
import array
import struct
import logging.config
logging.config.fileConfig("/opt/Keeprapid/KRWatch/server/conf/log.conf")
logger = logging.getLogger('krwatch')



class Consumer(threading.Thread):
    """docstring for Consumer"""
    def __init__(self, socketdict):
        super(Consumer, self).__init__()
        self.socketdict = socketdict
        fb = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fb)
        fb.close()

        fb = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
        self._config = json.load(fb)
        fb.close()
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.selfqname = 'A:Queue:MTPProxy'
        if self._config is not None and 'mtpdeviceproxy' in self._config and self._config['mtpdeviceproxy'] is not None:
            if 'Consumer_Queue_Name' in self._config['mtpdeviceproxy'] and self._config['mtpdeviceproxy']['Consumer_Queue_Name'] is not None:
                self.selfqname = self._config['mtpdeviceproxy']['Consumer_Queue_Name']


    def run(self):
        logger.debug("Start Consumer %s" % os.getpid())
        while 1:
            try:
                recvdata = self._redis.brpop(self.selfqname)
                if recvdata:
                    recvbuf = json.loads(recvdata[1])
                    # logger.debug(recvbuf)
                    if 'body' in recvbuf:
                        body = recvbuf['body']
                        if 'sockid' in body:
                            sockid = body['sockid']
                            imei = body.get('imei')
                            sendbuf = body['sendbuf']
                            arraysendbuf = array.array('B',)
                            arraysendbuf.fromlist(sendbuf)
                            if sockid in self.socketdict:
                                sock = self.socketdict[sockid]['sock']
        #                        self.socketdict.pop(recvbuf['sockid'])
#                                recvbuf.pop('sockid')
#                                recvbuf.pop('sender')
                                logger.debug("Send[%r]==>Device[%s][%r]" % (arraysendbuf.tostring(),imei,sock))
                                sock.sendall(arraysendbuf.tostring())
        #                        sock.shutdown(socket.SHUT_WR)
        #                        sock.close()
            except Exception as e:
                logger.debug("%s except raised : %s " % (e.__class__, e.args))

class TCPDogConsumer(threading.Thread):
    """docstring for TCPDogConsumer"""
    def __init__(self, socketdict):
        super(TCPDogConsumer, self).__init__()
        self.socketdict = socketdict
        fb = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fb)
        fb.close()

        fb = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
        self._config = json.load(fb)
        fb.close()
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.selfqname = 'W:Queue:TcpDogWatch'
        if self._config is not None and 'mtpdeviceproxy' in self._config and self._config['mtpdeviceproxy'] is not None:
            if 'Consumer_Queue_Name_TCP' in self._config['mtpdeviceproxy'] and self._config['mtpdeviceproxy']['Consumer_Queue_Name_TCP'] is not None:
                self.selfqname = self._config['mtpdeviceproxy']['Consumer_Queue_Name_TCP']


    def run(self):
        logger.debug("Start TCPDogConsumer %s" % os.getpid())
        try:
            while 1:
                recvdata = self._redis.brpop(self.selfqname)
                if recvdata:
                    recvbuf = json.loads(recvdata[1])
                    logger.debug("TCPDogConsumer: %r" %(recvbuf))
                    if 'body' in recvbuf:
                        body = recvbuf['body']
                        if 'sockid' in body:
                            sockid = body['sockid']
                            if sockid in self.socketdict:
                                sock = self.socketdict[sockid]['sock']
        #                        self.socketdict.pop(recvbuf['sockid'])
#                                recvbuf.pop('sockid')
#                                recvbuf.pop('sender')
                                logger.debug("close sock %r" % (sock))
                                sock.close()
                                self.socketdict.pop(sockid)
                            searchkey = "A:ds:%s:%s" % ('*',sockid)
                            resultlist = self._redis.keys(searchkey)
                            logger.debug("TCPDogConsumer delete sockkey %r" % (resultlist))
                            for key in resultlist:
                                self._redis.delete(key)
        #                        sock.shutdown(socket.SHUT_WR)
        #                        sock.close()
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))

class Dog2Thread(threading.Thread):

    '''监控responsesocketdict超时请求
       参数如下：
       responsesocketdict:等待响应的socket字典，内容为字典['sock', 'requestdatetime']
    '''
    def __init__(self, responsesocketdict, consumer, tcpdogonsumer):  # 定义构造器
        logger.debug("Created Dog2Thread instance")
        self.consumer = consumer
        self.tcpdogconsumer = tcpdogonsumer
        self.responsesocketdict = responsesocketdict
        threading.Thread.__init__(self)

    def run(self):
        logger.debug("Dog2Thread::run")
        while 1:
            if self.consumer is None or self.consumer.is_alive() is False:
                logger.error('Consumer is dead, dog run')
                self.consumer = Consumer(self.responsesocketdict)
                self.consumer.setDaemon(True)
                self.consumer.start()
            else:
                pass

            if self.tcpdogconsumer is None or self.tcpdogconsumer.is_alive() is False:
                logger.error('TCPDogConsumer is dead, dog run')
                self.tcpdogconsumer = TCPDogConsumer(self.responsesocketdict)
                self.tcpdogconsumer.setDaemon(True)
                self.tcpdogconsumer.start()
            else:
                pass

            time.sleep(10)

class DogThread(threading.Thread):

    '''监控responsesocketdict超时请求
       参数如下：
       responsesocketdict:等待响应的socket字典，内容为字典['sock', 'timestamp']
    '''
    def __init__(self, responsesocketdict):  # 定义构造器
        logger.debug("Created DogThread instance")
        threading.Thread.__init__(self)
        self._response_socket_dict = responsesocketdict
        self._timeoutsecond = 60*60

    def run(self):
        logger.debug("DogThread::run")

        while True:
            try:
                now = time.time()

                sortedlist = sorted(self._response_socket_dict.items(), key=lambda _response_socket_dict:_response_socket_dict[1]['timestamp'], reverse = 0)
#                logger.debug(sortedlist)
                for each in sortedlist:
                    key = each[0]
                    responseobj = each[1]
                    requestdatetime = responseobj['timestamp']
                    passedsecond = now - requestdatetime
                    if passedsecond > self._timeoutsecond:
                        sockobj = responseobj['sock']
                        logger.debug("DogThread close timeout sock[%s]%r" %(key, sockobj))
                        sockobj.close()
                        del(self._response_socket_dict[key])
                    else:
                        break
                logger.debug("DogThread finish check in %f" %(time.time()- now))
                time.sleep(60)

            except Exception as e:
                logger.warning("DogThread %s except raised : %s " % (
                    e.__class__, e.args))
                time.sleep(0.1)

def getValuebyStruct(structstr,buf):
    s = struct.Struct(structstr)
    return s.unpack(buf)[0]

def Handle(sock, address):
#    print os.getpid(),sock, address
#    print "%s" % sock.getsockname().__str__()
    fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()
    queuename = "W:Queue:MTPDataCenter"
    if _config is not None and 'mtpdatacenter' in _config and _config['mtpdatacenter'] is not None:
        if 'Consumer_Queue_Name' in _config['mtpdatacenter'] and _config['mtpdatacenter']['Consumer_Queue_Name'] is not None:
            queuename = _config['mtpdatacenter']['Consumer_Queue_Name']
    selfqueuename = "W:Queue:MTPProxy"
    if _config is not None and 'mtpdeviceproxy' in _config and _config['mtpdeviceproxy'] is not None:
        if 'Consumer_Queue_Name' in _config['mtpdeviceproxy'] and _config['mtpdeviceproxy']['Consumer_Queue_Name'] is not None:
            selfqueuename = _config['mtpdeviceproxy']['Consumer_Queue_Name']

    sockid = hashlib.md5(address.__str__()).hexdigest()
    if sockid not in socketdict:
        socketinfo = dict()
        socketinfo['sock'] = sock
        socketinfo['timestamp'] = time.time()
        socketdict[sockid] = socketinfo
        logger.debug("New Sock [%s]%r, total count = %d" % (sockid,sock, len(socketdict)))
    recvedbuf = ''
    recvedarray = array.array('B',)
    key_device_sock = "K:ds:%s"
    currentimei = ''
    while 1:
        try:
            recv = sock.recv(2048)
            # logger.debug("[%s]recv:%r" % (sockid, recv))
            if len(recv) == 0:
                logger.error("Connection close by peer")
                if sockid in socketdict:
                    socketdict.pop(sockid)
                    logger.debug("Delete Sock, total count = %d" % (len(socketdict)))
                if len(currentimei) > 0:
                    searchkey = key_device_sock % (currentimei)
                    r.delete(searchkey)
                # resultlist = r.keys(searchkey)
                # for key in resultlist:
                #     r.delete(key)

                sock.close()
                break
            temparray = array.array('B',recv)
            recvedarray = recvedarray+ temparray
            logger.debug('recvedarray = %r' % (recvedarray.tostring()))
            while len(recvedarray) > 4:
                # lentharray = array.array('H',recvedarray[0:2].tostring())
                msglen = getValuebyStruct('>H',recvedarray[0:2].tostring())
                cmdvalue = getValuebyStruct('>H',recvedarray[2:4].tostring())
                logger.debug('msglen = %d cmdvalue = %d' % (msglen,cmdvalue))
                if cmdvalue not in [0x0008, 0x0001, 0x0007]:
                    logger.error("invalid cmdvalue")
                    recvedarray = array.array('B',recv)
                    break

                if len(recvedarray) >= msglen:
                    #获取cmd 和 imei
                    msgarray = array.array('B',recvedarray[0:msglen].tostring())
                    # cmdarray = array.array('H',msgarray[2:4].tostring())
                    # logger.debug('lentharray = %r' % (lentharray))
                    # seq_id = array.array('I',msgarray[4:8].tostring())[0]
                    seq_id = getValuebyStruct('>I',msgarray[4:8].tostring())
                    # version = array.array('H',msgarray[8:10].tostring())[0]
                    version = getValuebyStruct('>H',msgarray[8:10].tostring())
                    secuirity = getValuebyStruct('B',msgarray[10:11].tostring())
                    # imei_len = array.array('B',msgarray[11:12].tostring())[0]
                    imei_len = getValuebyStruct('B',msgarray[11:12].tostring())
                    # imei_str = msgarray[12:12+imei_len].tostring().upper()
                    imei_str = getValuebyStruct('%ds'%(imei_len),msgarray[12:12+imei_len].tostring())

                    # devicetype = array.array('B',msgarray[].tostring())[0]
                    devicetype = getValuebyStruct('B',msgarray[12+imei_len:12+imei_len+1].tostring())
                    bodybeginlen = 13+imei_len
                
                    # logger.debug('msgarray = %r cmd  = %x'% (msgarray,cmdvalue))
                    msgdict = dict()
                    msgdict['action_cmd'] = 'device_upmsg'
                    msgbody = dict()
                    msgdict['body'] = msgbody
                    msgbody['recvedbuf'] = msgarray[bodybeginlen:].tolist()
                    msgbody['imei'] = imei_str
                    msgbody['devicetype'] = devicetype
                    msgbody['seq_id'] = seq_id
                    msgbody['version'] = version
                    msgbody['secuirity'] = secuirity
                    msgbody['cmdname'] = cmdvalue
                    msgbody['msglen'] = msglen
                    msgbody['bodylen'] = msglen - bodybeginlen
                    msgbody['sockid'] = sockid
                    msgdict['sockid'] = sockid
                    msgdict['from'] = ''
                    msgdict['version'] = '1.0'
                    # logger.debug(msgdict)
                    # logger.debug(r)
                    r.lpush(queuename,json.dumps(msgdict))
                    currentimei = imei_str
                    socketkey = key_device_sock % (imei_str)
                    r.set(socketkey,sockid)
                    r.expire(socketkey,60*60)
                    recvedarray = recvedarray[msglen:]
                else:
                    break

            socketdict[sockid]['timestamp'] = time.time()
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))
            sock.close()
            break


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        logger.debug("ThreadedTCPRequestHandler--->Handle[%r]"%(threading.current_thread()))
        logger.debug(self.client_address)
        # logger.debug(self.server)
        # logger.debug(self.server.socket)

        fileobj = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
        _json_dbcfg = json.load(fileobj)
        fileobj.close()
        fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
        _config = json.load(fileobj)
        fileobj.close()

        r = redis.StrictRedis(_json_dbcfg['redisip'], int(_json_dbcfg['redisport']),password=_json_dbcfg['redispassword'])

        queuename = "W:Queue:MTPDataCenter"
        if _config is not None and 'mtpdatacenter' in _config and _config['mtpdatacenter'] is not None:
            if 'Consumer_Queue_Name' in _config['mtpdatacenter'] and _config['mtpdatacenter']['Consumer_Queue_Name'] is not None:
                queuename = _config['mtpdatacenter']['Consumer_Queue_Name']

        sockid = hashlib.md5(self.client_address.__str__()).hexdigest()
        if sockid not in socketdict:
            socketinfo = dict()
            socketinfo['sock'] = self.request
            socketinfo['timestamp'] = time.time()
            socketdict[sockid] = socketinfo
            logger.debug("New Sock [%s]%r, total count = %d" % (sockid,self.request, len(socketdict)))
        recvedbuf = ''
        recvedarray = array.array('B',)
        key_device_sock = "K:ds:%s"
        currentimei = ''
        while 1:
            try:
                # recv = sock.recv(2048)
                recv = self.request.recv(2048)
                logger.debug("[%s]recv:%r" % (sockid, recv))
                if len(recv) == 0:
                    logger.error("Connection close by peer")
                    if sockid in socketdict:
                        socketdict.pop(sockid)
                        logger.debug("Delete Sock, total count = %d" % (len(socketdict)))
                    if len(currentimei) > 0:
                        searchkey = key_device_sock % (currentimei)
                        r.delete(searchkey)
                    # resultlist = r.keys(searchkey)
                    # for key in resultlist:
                    #     r.delete(key)
                    # sock.close()
                    break
                temparray = array.array('B',recv)
                recvedarray = recvedarray+ temparray
                logger.debug('recvedarray = %r' % (recvedarray.tostring()))
                while len(recvedarray) > 4:
                    # lentharray = array.array('H',recvedarray[0:2].tostring())
                    msglen = getValuebyStruct('>H',recvedarray[0:2].tostring())
                    cmdvalue = getValuebyStruct('>H',recvedarray[2:4].tostring())
                    logger.debug('msglen = %d cmdvalue = %d' % (msglen,cmdvalue))
                    if cmdvalue not in [0x0008, 0x0001, 0x0007]:
                        logger.error("invalid cmdvalue")
                        recvedarray = array.array('B',recv)
                        break

                    if len(recvedarray) >= msglen:
                        #获取cmd 和 imei
                        msgarray = array.array('B',recvedarray[0:msglen].tostring())
                        # cmdarray = array.array('H',msgarray[2:4].tostring())
                        # logger.debug('lentharray = %r' % (lentharray))
                        # seq_id = array.array('I',msgarray[4:8].tostring())[0]
                        seq_id = getValuebyStruct('>I',msgarray[4:8].tostring())
                        # version = array.array('H',msgarray[8:10].tostring())[0]
                        version = getValuebyStruct('>H',msgarray[8:10].tostring())
                        secuirity = getValuebyStruct('B',msgarray[10:11].tostring())
                        # imei_len = array.array('B',msgarray[11:12].tostring())[0]
                        imei_len = getValuebyStruct('B',msgarray[11:12].tostring())
                        # imei_str = msgarray[12:12+imei_len].tostring().upper()
                        imei_str = getValuebyStruct('%ds'%(imei_len),msgarray[12:12+imei_len].tostring())

                        # devicetype = array.array('B',msgarray[].tostring())[0]
                        devicetype = getValuebyStruct('B',msgarray[12+imei_len:12+imei_len+1].tostring())
                        bodybeginlen = 13+imei_len
                    
                        # logger.debug('msgarray = %r cmd  = %x'% (msgarray,cmdvalue))
                        msgdict = dict()
                        msgdict['action_cmd'] = 'device_upmsg'
                        msgbody = dict()
                        msgdict['body'] = msgbody
                        msgbody['recvedbuf'] = msgarray[bodybeginlen:].tolist()
                        msgbody['imei'] = imei_str
                        msgbody['devicetype'] = devicetype
                        msgbody['seq_id'] = seq_id
                        msgbody['version'] = version
                        msgbody['secuirity'] = secuirity
                        msgbody['cmdname'] = cmdvalue
                        msgbody['msglen'] = msglen
                        msgbody['bodylen'] = msglen - bodybeginlen
                        msgbody['sockid'] = sockid
                        msgdict['sockid'] = sockid
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        # logger.debug(msgdict)
                        # logger.debug(r)
                        r.lpush(queuename,json.dumps(msgdict))
                        currentimei = imei_str
                        socketkey = key_device_sock % (imei_str)
                        r.set(socketkey,sockid)
                        r.expire(socketkey,60*60)
                        recvedarray = recvedarray[msglen:]
                    else:
                        break

                socketdict[sockid]['timestamp'] = time.time()
            except Exception as e:
                logger.debug("%s except raised : %s " % (e.__class__, e.args))
                # sock.close()
                break
        # data = self.request.recv(10240)
        # logger.debug(data)
        
        # response = "{}: {}".format(cur_thread.name, data)
        # self.request.sendall(response)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):#继承ThreadingMixIn表示使用多线程处理request，注意这两个类的继承顺序不能变
    pass


if __name__ == "__main__":

    fileobj = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
    _json_dbcfg = json.load(fileobj)
    fileobj.close()

    fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()
    # r = redis.StrictRedis(_json_dbcfg['redisip'], int(_json_dbcfg['redisport']),password=_json_dbcfg['redispassword'])

    queuename = "W:Queue:MTPProxy"
    if _config is not None and 'mtpdeviceproxy' in _config and _config['mtpdeviceproxy'] is not None:
        if 'Consumer_Queue_Name' in _config['mtpdeviceproxy'] and _config['mtpdeviceproxy']['Consumer_Queue_Name'] is not None:
            queuename = _config['mtpdeviceproxy']['Consumer_Queue_Name']

    listen_port = 8082
    threadcount = 2
    if _config is not None and 'mtpdeviceproxy' in _config and _config['mtpdeviceproxy'] is not None:
        if 'Listen_Port' in _config['mtpdeviceproxy'] and _config['mtpdeviceproxy']['Listen_Port'] is not None:
            listen_port = int(_config['mtpdeviceproxy']['Listen_Port'])
        if 'thread_count' in _config['mtpdeviceproxy'] and _config['mtpdeviceproxy']['thread_count'] is not None:
            threadcount = int(_config['mtpdeviceproxy']['thread_count'])


    socketdict = dict()

    dogAgent = DogThread(socketdict)
    dogAgent.setDaemon(True)
    dogAgent.start()

    dog2Agent = Dog2Thread(socketdict,None, None)
    dog2Agent.setDaemon(True)
    dog2Agent.start()


    # server = StreamServer(("",listen_port), Handle)
    # server.serve_forever()

    server = ThreadedTCPServer(("", listen_port), ThreadedTCPRequestHandler)
    server.serve_forever()

    # server_thread = threading.Thread(target=server.serve_forever)
    # server_thread.daemon = True
    # server_thread.start()
    # logger.debug("Server loop running in thread: %r"%(server_thread.name))
