#!/usr/bin/python
# -*- coding: UTF-8 -*-
# filename:  httpgeproxy.py
# creator:   gabriel.liao
# datetime:  2013-6-17

"""http proxy server base gevent server"""
# from gevent import monkey
# import gevent
# monkey.patch_socket()
# monkey.patch_thread()
# monkey.patch_os()
# from gevent import socket
# from gevent.server import StreamServer
from multiprocessing import Process
import SocketServer
import logging
import logging.config
import threading
import uuid
import time
import sys
from Queue import Queue
import redis
import os
import json

try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser

logging.config.fileConfig("/opt/Keeprapid/KRWatch/server/conf/log.conf")
logger = logging.getLogger('krwatch')

class HttpProxyConsumer(threading.Thread):

    def __init__(self, responsesocketdict):  # 定义构造器
        logger.debug("Created HttpProxyConsumer instance")
        threading.Thread.__init__(self)
        self._response_socket_dict = responsesocketdict
        fileobj = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])


    def procdata(self, recvdata):
        recvbuf = json.loads(recvdata)
        if 'sockid' in recvbuf:
#            logger.debug(self._response_socket_dict)
            if recvbuf['sockid'] in self._response_socket_dict:
                respobj = self._response_socket_dict.pop(recvbuf['sockid'])
                # logger.debug("HttpProxyConsumer::----> responsesocketdict len = %d", len(self._response_socket_dict))
#                logger.debug(respobj)
                if respobj is not None and 'sock' in respobj:
                    respsockobj = respobj['sock']
#                    logger.debug("Recver callback [%s]" % (recvdata))
                    recvbuf.pop('sockid')
                    recvbuf.pop('from')
                    respsockobj.sendall('HTTP/1.1 200 OK\n\n%s' % (json.dumps(recvbuf)))
                    # respsockobj.shutdown(socket.SHUT_WR)
                    respsockobj.close()


    def run(self):
        queuename = "W:Queue:httpproxy"
        if self._config is not None and 'httpproxy' in self._config and self._config['httpproxy'] is not None:
            if 'Consumer_Queue_Name' in self._config['httpproxy'] and self._config['httpproxy']['Consumer_Queue_Name'] is not None:
                queuename = self._config['httpproxy']['Consumer_Queue_Name']

        listenkey = "%s:%s" % (queuename, os.getpid())
        logger.debug("HttpProxyConsumer::run listen key = %s" % (listenkey))
        while 1:
            try:
                recvdata = self._redis.brpop(listenkey)
#                logger.debug(recvdata)
                self.procdata(recvdata[1])

            except Exception as e:
                logger.error("HttpProxyConsumer %s except raised : %s " % (e.__class__, e.args))
                time.sleep(1)


class PublishThread(threading.Thread):

    '''接收客户http请求内容，转换为封装AMPQ协议，采用Publish Api发送请求，
           并把socket对象和时间压入等待响应字典
       参数如下：
       httpclientsocketqueue:客户请求socket队列
       publisher:发送AMPQ消息对象
       responsesocketdict:等待响应的socket字典，内容为字典['sock', 'requestdatetime']
       recvbuflen:每次接收http请求内容长度，默认2048
    '''
    def __init__(self, httpclientsocketqueue, responsesocketdict, recvbuflen):  # 定义构造器
        logger.debug("Created PublishThread instance")
        threading.Thread.__init__(self)
        self._httpclientsocketqueue = httpclientsocketqueue
        self._response_socket_dict = responsesocketdict
        self._recvbuflen = recvbuflen
        fileobj = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])

    def run(self):
        queuename = "W:Queue:httpproxy"
        if self._config is not None and 'httpproxy' in self._config and self._config['httpproxy'] is not None:
            if 'Consumer_Queue_Name' in self._config['httpproxy'] and self._config['httpproxy']['Consumer_Queue_Name'] is not None:
                queuename = self._config['httpproxy']['Consumer_Queue_Name']

        selfqueuename = "%s:%s" % (queuename, os.getpid())
        logger.debug("PublishThread::run : %s" % (selfqueuename))
        servicelist = os.listdir('./apps')
        while True:
            try:
                sockobj = self._httpclientsocketqueue.get()
                request_path = ""
                body = []
                p = HttpParser()
                seqid = uuid.uuid1()
                requestdict = dict()
                requestdict['sock'] = sockobj
                requestdatetime = time.strftime(
                    '%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))
                requestdict['requestdatetime'] = requestdatetime
                self._response_socket_dict[seqid.__str__()] = requestdict
                logger.debug("responsesocketdict len = %d", len(self._response_socket_dict))

                while True:
                    request = sockobj.recv(self._recvbuflen)
                    # logger.warning("request  : %s" % (request))

                    recved = len(request)
                    # logger.warning("recved   : %d" % (recved))

                    if(recved == 0):
#                        logger.warning("socket is closed by peer")
                        sockobj.close()
                        break

                    nparsed = p.execute(request, recved)
                    # logger.warning("nparsed  : %d" % (nparsed))
                    if nparsed != recved:
                        logger.warning("parse error")
                        sockobj.close()
                        break

#                    if p.is_headers_complete():
#                        request_headers = p.get_headers()
#                        for key in request_headers:
#                            logger.debug("%s: %s" % (key, request_headers[key]))

#                        logger.warning("headers complete")

                    if p.is_partial_body():
                        body.append(p.recv_body())
                        # logger.warning("body  : %s" % (body))

                    if p.is_message_complete():
#                        logger.warning("message complete")
                        break

                content = "".join(body)


                routekey = ""
                servicepath = ""

                # 如果是/xxx格式认为是route key，如果是/xxx/yyy/zzz格式认为是dest service
                request_path = p.get_path()[1:]

                # logger.warning('PublishThread request_path (%s), is routekey (%d)' % (request_path, request_path.find('/')))
                # logger.debug("content : %s" % (content))

                if request_path.find('/') == -1 and len(request_path) and request_path in servicelist:

                    routekey = "W:Queue:%s" % request_path
                    if request_path in self._config:
                        routekey = self._config[request_path]['Consumer_Queue_Name']

                    if len(content) == 0:
                        content_json = dict()
                    else:
                        content_json = json.loads(content)

                    content_json['sockid'] = seqid.__str__()
                    content_json['from'] = selfqueuename
                    self._redis.lpush(routekey, json.dumps(content_json))
                else:
                    ret = dict()
                    ret['error_code'] = '40004'
                    sockobj.sendall('HTTP/1.1 200 OK\n\n%s' % (json.dumps(ret)))
#                    sockobj.shutdown(socket.SHUT_WR)
                    sockobj.close()
                    self._response_socket_dict.pop(seqid.__str__())
                    continue



                # sockobj.sendall('HTTP/1.1 200 OK\n\nWelcome %s' % (
                #    seqid))
                # sockobj.close()

            except Exception as e:
                logger.error("PublishThread %s except raised : %s " % (
                    e.__class__, e.args))


class DogThread(threading.Thread):

    '''监控responsesocketdict超时请求
       参数如下：
       responsesocketdict:等待响应的socket字典，内容为字典['sock', 'requestdatetime']
    '''
    def __init__(self, responsesocketdict):  # 定义构造器
        logger.debug("Created DogThread instance")
        threading.Thread.__init__(self)
        self._response_socket_dict = responsesocketdict
        self._timeoutsecond = 10

    def _isoString2Time(self, s):
        '''
        convert a ISO format time to second
        from:2006-04-12 16:46:40 to:23123123
        把一个时间转化为秒
        '''
        ISOTIMEFORMAT = '%Y.%m.%d.%H.%M.%S'
        return time.strptime(s, ISOTIMEFORMAT)

    def calcPassedSecond(self, s1, s2):
        '''
        convert a ISO format time to second
        from:2006-04-12 16:46:40 to:23123123
        把一个时间转化为秒
        '''
        s1 = self._isoString2Time(s1)
        s2 = self._isoString2Time(s2)
        return time.mktime(s1) - time.mktime(s2)

    def run(self):
        logger.debug("DogThread::run")

        while True:
            try:
                now = time.strftime(
                    '%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))

                sorted(self._response_socket_dict.items(), key=lambda _response_socket_dict:_response_socket_dict[1]['requestdatetime'])

                for key in self._response_socket_dict.keys():
                    responseobj = self._response_socket_dict[key]
                    requestdatetime = responseobj['requestdatetime']
                    passedsecond = self.calcPassedSecond(now, requestdatetime)
                    if passedsecond > self._timeoutsecond:
                        logger.warn("dog::close sock %s",key)
                        del(self._response_socket_dict[key])
                        sockobj = responseobj['sock']
                        server = responseobj['server']
                        sockobj.sendall('HTTP/1.1 500 OK\n\nTimeout %s' % (key))
                        # sockobj.close()
                        server.shutdown_request(sockobj)

                    else:
                        break

                time.sleep(0.1)

            except Exception as e:
                logger.warning("DogThread %s except raised : %s " % (
                    e.__class__, e.args))
                time.sleep(0.1)

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        thd = threading.current_thread()
        # logger.debug("ThreadedTCPRequestHandler--->Handle[%r]"%(thd))
        # logger.debug(dir(thd))
        # logger.debug(self.client_address)
        # logger.debug(dir(self.server))
        # logger.debug(dir(self.request))
        # logger.debug(self.request.__class__)

        # logger.debug(self.server.socket)

        fileobj = open('/opt/Keeprapid/KRWatch/server/conf/db.conf', 'r')
        _json_dbcfg = json.load(fileobj)
        fileobj.close()
        fileobj = open("/opt/Keeprapid/KRWatch/server/conf/config.conf", "r")
        _config = json.load(fileobj)
        fileobj.close()

        self._redis = redis.StrictRedis(_json_dbcfg['redisip'], int(_json_dbcfg['redisport']),password=_json_dbcfg['redispassword'])

        queuename = "W:Queue:httpproxy"
        if _config is not None and 'httpproxy' in _config and _config['httpproxy'] is not None:
            if 'Consumer_Queue_Name' in _config['httpproxy'] and _config['httpproxy']['Consumer_Queue_Name'] is not None:
                queuename = _config['httpproxy']['Consumer_Queue_Name']

        servicelist = os.listdir('./apps')
        try:
        # if 1:
            # sockobj = self._httpclientsocketqueue.get()
            request_path = ""
            body = []
            p = HttpParser()
            seqid = uuid.uuid1()
            # requestdict = dict()
            # requestdict['sock'] = self.request
            # requestdict['server'] = self.server
            # requestdatetime = time.strftime('%Y.%m.%d.%H.%M.%S', time.localtime(time.time()))
            # requestdict['requestdatetime'] = requestdatetime
            # responsesocketdict[seqid.__str__()] = requestdict
            # logger.debug("responsesocketdict len = %d", len(responsesocketdict))
            selfqueuename = "%s:%s" % (queuename, seqid.__str__())
            logger.debug("ThreadedTCPRequestHandler::run : %s" % (selfqueuename))

            while True:
                self.request.settimeout(10)
                request = self.request.recv(recv_buf_len)
                # logger.warning("request  : %s" % (request))
                recved = len(request)
                # logger.warning("recved   : %d" % (recved))
                if(recved == 0):
                    logger.warning("socket is closed by peer")
                    self.request.close()
                    return

                nparsed = p.execute(request, recved)
                # logger.warning("nparsed  : %d" % (nparsed))
                if nparsed != recved:
                    logger.warning("parse error")
                    self.request.sendall('HTTP/1.1 500 OK\n\n')
                    self.request.close()
                    break

                if p.is_partial_body():
                    body.append(p.recv_body())
                    # logger.warning("body  : %s" % (body))

                if p.is_message_complete():
#                        logger.warning("message complete")
                    break

            content = "".join(body)

            routekey = ""
            servicepath = ""

            # 如果是/xxx格式认为是route key，如果是/xxx/yyy/zzz格式认为是dest service
            request_path = p.get_path()[1:]

            # logger.warning('ThreadedTCPRequestHandler request_path (%s), is routekey (%d)' % (request_path, request_path.find('/')))
            # logger.debug("content : %s" % (content))
            if content == '':
                self.request.close()
                # responsesocketdict.pop(seqid.__str__())
                return

            if request_path.find('/') == -1 and len(request_path) and request_path in servicelist:

                routekey = "W:Queue:%s" % request_path
                if request_path in _config:
                    routekey = _config[request_path]['Consumer_Queue_Name']

                if len(content) == 0:
                    content_json = dict()
                else:
                    content_json = json.loads(content)

                content_json['sockid'] = seqid.__str__()
                content_json['from'] = selfqueuename
                self._redis.lpush(routekey, json.dumps(content_json))
                #进入接收模块
                t1 = time.time()
                while 1:
                    if self._redis.llen(selfqueuename) >0:
                        recvdata = self._redis.rpop(selfqueuename)
                        # logger.debug("ThreadedTCPRequestHandler:%r",recvdata)
                        recvbuf = json.loads(recvdata)
                        recvbuf.pop('sockid')
                        recvbuf.pop('from')
                        self.request.sendall('HTTP/1.1 200 OK\n\n%s' % (json.dumps(recvbuf)))
                        self.request.close()
                        return
                    time.sleep(0.1)
                    t2 = time.time()
                    if t2-t1 > 10:
                        #超时未返回
                        logger.error("ThreadedTCPRequestHandler: Waiting...... TIMEOUT")
                        self.request.sendall('HTTP/1.1 500 OK\n\n%s' % (json.dumps(recvbuf)))
                        self.request.close()
                        return
            else:
                ret = dict()
                ret['error_code'] = '40004'
                self.request.sendall('HTTP/1.1 200 OK\n\n%s' % (json.dumps(ret)))
#                    sockobj.shutdown(socket.SHUT_WR)
                self.request.close()
                # responsesocketdict.pop(seqid.__str__())
                return

        except Exception as e:
            logger.error("ThreadedTCPRequestHandler %s except raised : %s " % (
                e.__class__, e.args))
            self.request.close()
            return
        # data = self.request.recv(10240)
        # logger.debug(data)
        
        # response = "{}: {}".format(cur_thread.name, data)
        # self.request.sendall(response)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):#继承ThreadingMixIn表示使用多线程处理request，注意这两个类的继承顺序不能变
    timeout = 10

if __name__ == "__main__":

    httpclientsocketqueue = Queue()
    responsesocketdict = dict()

    # def recvrawsocket(sockobj, address):
    #     '''
    #     接收客户http请求，将socket对象压入publish队列，由PublishThread处理
    #     '''
    #     # logger.debug("before queuelenth = %d", httpclientsocketqueue.qsize())
    #     httpclientsocketqueue.put(sockobj)
    #     # logger.debug("queuelenth = %d", httpclientsocketqueue.qsize())

    recv_buf_len = 12040
    port = 8083

    dogAgent = DogThread(responsesocketdict)
    dogAgent.setDaemon(True)
    dogAgent.start()

    # publishAgent = PublishThread(
    #     httpclientsocketqueue, responsesocketdict, recv_buf_len)
    # publishAgent.setDaemon(True)
    # publishAgent.start()

    response_count = 1
    for i in range(0,response_count):
        publishConsumer = HttpProxyConsumer(responsesocketdict)
        publishConsumer.setDaemon(True)
        publishConsumer.start()


    # logger.error('Http gevent proxy serving on %d...' % (port))
    # server = StreamServer(('', port), recvrawsocket, backlog=100000)
    # server.serve_forever()

    server = ThreadedTCPServer(("", port), ThreadedTCPRequestHandler)
    server.timeout = 5
    server.serve_forever()

