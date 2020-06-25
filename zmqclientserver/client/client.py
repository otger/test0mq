#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import zmq
import threading

import json
from zmqclientserver.common.message import MessageBase, CommandMSG
from zmqclientserver.client.callbacks import AsyncCallback
from zmqclientserver.logs import get_logger

log = get_logger('sillyclient')


class AsyncStats(object):
    txt_list = ['bad_msg', 'cb_exc', 'cb_calls', 'cb_reg']

    def __init__(self):
        self._topics = {}
        self._unknown_topics = {}

    def new_topic(self, topic):
        topic = str(topic)
        if topic not in self._topics:
            self._topics[topic] = {k: 0 for k in self.txt_list}

    def bad_msg(self, topic):
        """ Received a MSG from a topic which could not be recreated from its json bytes"""
        topic = str(topic)
        self._topics[topic]['bad_msg'] += 1

    def exception(self, topic):
        """Exception on callback"""
        topic = str(topic)
        self._topics[topic]['cb_exc'] += 1

    def cb_call(self, topic):
        """Calling callbacks on topic"""
        topic = str(topic)
        self._topics[topic]['cb_calls'] += 1

    def cb_reg(self, topic):
        """A new callback has been registered"""
        topic = str(topic)
        self._topics[topic]['cb_reg'] += 1

    def unknown_topic(self, topic):
        """A message for an unknown topic has been received"""
        topic = str(topic)
        if topic not in self._unknown_topics:
            self._unknown_topics[topic] = 0
        self._unknown_topics[topic] += 1

    def as_json(self):
        return json.dumps({'registered': self._topics, 'unknown': self._unknown_topics})


class zmqClient(object):
    def __init__(self, en_async=True, async_timeout=2000):
        self._ctx = None
        self._req_socket = None
        self._sub_socket = None
        self._async_enabled = en_async
        self._exit = False
        self._async_th = None
        self._as = AsyncStats()
        self._async_timeout = async_timeout
        self._callbacks = {}

    def connect(self, ip='localhost'):
        self._ctx = zmq.Context()
        #  Socket to talk to server
        self._req_socket = self._ctx.socket(zmq.REQ)
        self._req_socket.connect(f"tcp://{ip}:5555")

        if self._async_enabled:
            self._sub_socket = self._ctx.socket(zmq.SUB)
            self._sub_socket.connect(f"tcp://{ip}:5556")
            self._sub_socket.setsockopt(zmq.RCVTIMEO, self._async_timeout)
        log.info('Initialized sockets')

    def close(self):
        if self._async_th:
            self._exit = True
            log.debug('Set to exit loop')
            self._async_th.join()
            log.debug('Joined pubsub thread')
        if self._req_socket:
            self._req_socket.close()
            self._req_socket = None
        if self._sub_socket:
            self._sub_socket.close()
            self._sub_socket = None
        if self._ctx:
            self._ctx = None

    def async_subscribe(self, topic, callback):
        if self._async_enabled:
            log.debug(f"Petition to subscribe to topic '{topic}'")
            if not isinstance(topic, bytes):
                topic = topic.encode('utf-8')

            self._as.new_topic(topic)

            if not issubclass(type(callback), AsyncCallback):
                raise Exception("callback must be of type AsyncCallback")
            if topic not in self._callbacks:
                self._callbacks[topic] = []
            self._callbacks[topic].append(callback)
            self._sub_socket.subscribe(topic)
            self._as.cb_reg(topic)
            log.debug(f"Subscribed callback to topic '{topic}'")
        else:
            log.error(f"Tried to subscribe to topic '{topic}' but async is not enabled")

    def _process_callbacks(self, topic, async_msg):
        if topic in self._callbacks:
            for cb in self._callbacks[topic]:
                try:
                    cb.run(topic, async_msg)
                    self._as.cb_call(topic)
                except:
                    log.exception(f"Exception when executing callback for topic '{topic}'")
                    self._as.exception(topic)
        else:
            self._as.unknown_topic(topic)

    def async_loop(self):
        if self._async_enabled:
            if not self._sub_socket:
                raise Exception('Async socket not initialized')
            while not self._exit:
                try:
                    topic, msg_bytes = self._sub_socket.recv_multipart()
                    log.debug(f"Received async for topic '{topic}'")
                except zmq.error.Again:
                    continue
                else:
                    try:
                        msg = MessageBase.from_bytes(msg_bytes=msg_bytes)
                    except:
                        log.exception('Exception when reconstructing message')
                        self._as.bad_msg(topic)
                    else:
                        self._process_callbacks(topic=topic, async_msg=msg)
            log.info('Out of pubsub loop')
        else:
            log.error('Async loop was executed but async is not enabled')

    def start(self):
        self._exit = False
        if self._async_enabled:
            self._async_th = threading.Thread(target=self.async_loop)
            self._async_th.start()
            log.info("Started pubsub thread")
        else:
            log.error('Async is not enabled, can\'t start its thread')

    def _command(self, cmd):
        if not issubclass(type(cmd), CommandMSG):
            log.debug('Bad command, command should be of type CommandMSG')
            raise Exception("Bad command, command should be of type CommandMSG")
        self._req_socket.send(cmd.as_bytes)
        ans_bytes = self._req_socket.recv()
        # log.debug(f"Received answer from server for command '{cmd.command}")
        return MessageBase.from_bytes(ans_bytes)

    def get_async_stats(self):
        return self._as.as_json()
