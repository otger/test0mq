#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A simple python script

"""
__author__ = 'Otger Ballester'
__copyright__ = 'Copyright 2020'
__date__ = '22/06/2020'
__credits__ = ['Otger Ballester', ]
__license__ = 'GPL'
__version__ = '0.1'
__maintainer__ = 'Otger Ballester'
__email__ = 'otger@ifae.es'

from abc import ABC, abstractmethod


class AsyncCallback(ABC):
    def __init__(self):
        self._num_calls = 0
        self._run_errors = 0
        self._last_call = None

    @abstractmethod
    def _run(self, key, msg):
        pass

    def run(self, key, msg):
        self._num_calls += 1
        try:
            self._run(key, msg)
        except:
            self._run_errors += 1
            raise


class PrintCB(AsyncCallback):

    def _run(self, key, msg):
        print(f"{key}: {msg.name} - {msg.value} - {msg.ts}")


if __name__ == '__main__':
    pass
