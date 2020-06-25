#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A simple python script

"""
__author__ = 'Otger Ballester'
__copyright__ = 'Copyright 2020'
__date__ = '23/06/2020'
__credits__ = ['Otger Ballester', ]
__license__ = 'GPL'
__version__ = '0.1'
__maintainer__ = 'Otger Ballester'
__email__ = 'otger@ifae.es'

from zmqclientserver.client.client import zmqClient
from zmqclientserver.common.message import CommandMSG
from example.commands import CommandTypes


class SillyClient(zmqClient):
    def __init__(self, en_async=True):
        super().__init__(en_async=en_async)

    def close_server(self):
        cmd = CommandMSG(command=CommandTypes.exit, args=[True])
        return self._command(cmd)

    def sum(self, a, b):
        return self._command(CommandMSG(command=CommandTypes.sum, args=[a, b]))

    def add_async_topic(self, topic):
        cmd = CommandMSG(command=CommandTypes.pub, args=[topic])
        return self._command(cmd)


if __name__ == "__main__":
    import time
    from zmqclientserver.client.callbacks import PrintCB
    from zmqclientserver.logs import get_logger, log_add_stream_handler
    import random
    log = get_logger('funclient')
    log_add_stream_handler()

    c = SillyClient()
    c.connect()
    c.start()
    c.async_subscribe('zip.101', PrintCB())
    print(c.sum(1, 2).as_json)
    c.add_async_topic('whatever')
    c.async_subscribe('extra.whatever', PrintCB())

    start = time.time()
    num_cmds = 5000
    for i in range(num_cmds):
        c.sum(random.randrange(0, 50), random.randrange(0, 50))
    log.info(f"Took {time.time()-start:.3f} to send {num_cmds} requests")

    # c.close_server()

    c.close()
    log.info('Exited client')
    log.info(c.get_async_stats())

