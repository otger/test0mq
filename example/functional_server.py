#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A simple python script

"""
__author__ = 'Otger Ballester'
__copyright__ = 'Copyright 2020'
__date__ = '25/06/2020'
__credits__ = ['Otger Ballester', ]
__license__ = 'GPL'
__version__ = '0.1'
__maintainer__ = 'Otger Ballester'
__email__ = 'otger@ifae.es'

from daq.logs import get_logger
from daq.server.server import zmqServer

import threading
import numbers
import time
from random import randrange

from example.commands import CommandTypes

log = get_logger('funserver')


class SillyServer(zmqServer):

    def __init__(self, pub_delay=0.005):
        super().__init__()
        self.pub_delay = pub_delay
        self.extra_pubs = []

    def start(self):
        super(SillyServer, self).start()
        pub_hell_th = threading.Thread(target=self._pub_hell_worker, name='rep_req')
        pub_hell_th.start()
        log.info('Started pub_hell_th thread')
        self._th.append(pub_hell_th)

    def _pub_hell_worker(self):
        while not self._exit:
            self.pub_zipcode()
            self.pub_extrasyncs()
            time.sleep(self.pub_delay)

    def pub_zipcode(self):
        zipcode = randrange(1, 1000)
        temperature = randrange(-80, 135)
        humidity = randrange(10, 60)
        self.async_pub(topic=f"zip.{zipcode}", value={'temp': temperature, 'hum': humidity, 'loop': self.pub_loop,
                                                      'q': self._pub_queue.qsize()})

    def pub_extrasyncs(self):
        if self.pub_loop % 1000 == 0:
            for k in self.extra_pubs:
                self.async_pub(topic=f"extra.{k}",
                               value={'value': randrange(1, 100), 'loop': self.pub_loop})

    def execute_command(self, cmd_msg):
        if cmd_msg.command == CommandTypes.exit:
            self._exit = cmd_msg.args[0]
            return 'Done', None
        elif cmd_msg.command == CommandTypes.sum:
            if isinstance(cmd_msg.args[0], numbers.Number) and isinstance(cmd_msg.args[1], numbers.Number):
                return cmd_msg.args[0] + cmd_msg.args[1], None
            else:
                return None, "Non numeric arguments"
        elif cmd_msg.command == CommandTypes.pub:
            if isinstance(cmd_msg.args[0], str):
                self.extra_pubs.append(cmd_msg.args[0])
                return "Done", None
            else:
                return None, "Non string argument"
        elif cmd_msg.command == CommandTypes.long:
            try:
                delay = float(cmd_msg.args[0])
                time.sleep(delay)
                return "Done", None
            except:
                return None, "Wrong argument, must be numeric"

        else:
            return None, f"Unknown command '{cmd_msg.command}'"


if __name__ == "__main__":
    from daq.logs import log_add_stream_handler
    log_add_stream_handler()
    s = SillyServer()
    s.init_sockets()
    s.start()
    log.info("Started server")
    s.join()
    log.info("joined threads")
