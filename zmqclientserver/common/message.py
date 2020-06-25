#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A simple python script

"""
__author__ = 'Otger Ballester'
__copyright__ = 'Copyright 2020'
__date__ = '16/06/2020'
__credits__ = ['Otger Ballester', ]
__license__ = 'GPL'
__version__ = '0.1'
__maintainer__ = 'Otger Ballester'
__email__ = 'otger@ifae.es'

import datetime
import json
import time
import uuid
from abc import ABC, abstractmethod

from zmqclientserver.logs import get_logger

log = get_logger('common.message')


class MessageType(object):
    ASYNC = 0x1
    CMD = 0x2
    CMDANS = 0x3
    ERROR = 0x4


class MessageBase(ABC):
    def __init__(self, msg_type=MessageType.ASYNC, uid=None):
        self._type = msg_type
        if uid:
            # log.debug(f"Constructing message with provided uid. Type '{type(uid)} - value '{uid}")
            if isinstance(uid, uuid.UUID):
                self._uid = uid
            else:
                self._uid = uuid.UUID(uid)
        else:
            self._uid = uuid.uuid4()

    @property
    def uid(self):
        return self._uid

    @ property
    def type(self):
        return self._type

    @property
    @abstractmethod
    def as_dict(self):
        # must return a dict type with three elements: msg_type, uid, contents
        # contents must be a dict with as many keys as needed by the class to recompose itself
        pass

    @property
    def base_dict(self):
        return {'msg_type': self.type,
                'uid': self.uid.hex,
                'contents': {}}

    @property
    def as_json(self):
        return json.dumps(self.as_dict)

    @property
    def as_bytes(self):
        return self.as_json.encode('utf-8')

    @classmethod
    def from_json(cls, json_string):
        d = json.loads(json_string)
        kwargs = d['contents']
        if d['msg_type'] == MessageType.CMD:
            return CommandMSG(uid=d['uid'], **kwargs)
        elif d['msg_type'] == MessageType.ASYNC:
            return AsyncMSG(uid=d['uid'], **kwargs)
        elif d['msg_type'] == MessageType.CMDANS:
            return CommandAns(uid=d['uid'], **kwargs)
        elif d['msg_type'] == MessageType.ERROR:
            return ErrorMSG(uid=d['uid'], **kwargs)
        raise Exception(f"Unknown message type {d['msg_type']}")

    @classmethod
    def from_bytes(cls, msg_bytes):
        return cls.from_json(msg_bytes.decode('utf-8'))


class CommandMSG(MessageBase):

    def __init__(self, command, args=None, kwargs=None, uid=None):
        # args and kwargs contents must be json serializable items
        super().__init__(msg_type=MessageType.CMD, uid=uid)
        if kwargs is None:
            kwargs = {}
        if args is None:
            args = []
        self.command = command
        self.args = args
        self.kwargs = kwargs

    @property
    def as_dict(self):
        tmp = self.base_dict
        tmp['contents']['command'] = self.command
        tmp['contents']['args'] = self.args
        tmp['contents']['kwargs'] = self.kwargs
        return tmp


class CommandAns(CommandMSG):
    def __init__(self, command, args=None, kwargs=None, uid=None, ans=None, error=None, started=None, ended=None):
        super().__init__(command, args, kwargs, uid)
        self._type = MessageType.CMDANS
        self.ans = ans
        self.error = error
        self._started_at = started
        self._ended_at = ended

    @classmethod
    def from_cmdmsg(cls, cmdmsg):
        return cls(command=cmdmsg.command, args=cmdmsg.args,
                   kwargs=cmdmsg.kwargs, uid=cmdmsg.uid)

    def set_start_time(self, start=None):
        self._started_at = start or time.time()

    def set_end_time(self, end=None):
        self._ended_at = end or time.time()

    @property
    def as_dict(self):
        tmp = super(CommandAns, self).as_dict
        tmp['contents']['ans'] = self.ans
        tmp['contents']['error'] = self.error
        if self._started_at:
            tmp['contents']['started'] = self._started_at
        if self._ended_at:
            tmp['contents']['ended'] = self._ended_at
        return tmp


class ErrorMSG(MessageBase):
    def __init__(self, msg, uid=None):
        super().__init__(msg_type=MessageType.ERROR, uid=uid)
        self.msg = msg

    @property
    def as_dict(self):
        tmp = self.base_dict
        tmp['contents']['msg'] = self.msg
        return tmp


class AsyncMSG(MessageBase):
    def __init__(self, name, value, uid=None):
        # value must be a serializable item
        super().__init__(msg_type=MessageType.ASYNC, uid=uid)
        self.name = name
        self.value = value
        self.ts = datetime.datetime.utcnow().timestamp()

    @property
    def as_dict(self):
        tmp = self.base_dict
        tmp['contents']['name'] = self.name
        tmp['contents']['value'] = self.value
        return tmp


if __name__ == "__main__":

    am = AsyncMSG(name='humidity', value=58.9)
    print(am.as_json)
    am2 = MessageBase.from_json(am.as_json)
    print(f"Type of am: {type(am)} - am2: {type(am2)}")

    print(am.uid == am2.uid)

    cm = CommandMSG(command='exit')
    print(cm.as_json)
    cm2 = MessageBase.from_json(cm.as_json)
    print(f"Type of cm: {type(cm)} - cm2: {type(cm2)}")

    cm = CommandMSG(command='sum', args=(1, 2))
    print(cm.as_json)
    print(cm.as_bytes)
    cm2 = MessageBase.from_json(cm.as_json)
    cm3 = MessageBase.from_bytes(cm2.as_bytes)
    print(f"Type of cm: {type(cm)} - cm2: {type(cm2)} - cm3: {type(cm3)}")


