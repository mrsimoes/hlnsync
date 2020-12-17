#!/usr/bin/env python

# Copyright (C) 2020 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
An abstract class that provides producer-consumer synchronized threading with a
buffer size of one datum.
"""

import threading

import abc

class NoMoreData(Exception):
    pass

class ProducerConsumerThreaded:
    """
    Coordinate a Producer thread with a Consumer thread.
    """
    def __init__(self):
        self.ready_for_data = threading.Event()
        self.ready_for_data.set()
        self.data_is_available = threading.Event()
        self.done = False
        def producer_fn():
            while True:
                try:
                    datum = self.produce()
                except NoMoreData:
                    self.ready_for_data.wait()
                    self.done = True
                    self.data_is_available.set()
                    break
                self.ready_for_data.wait()
                self.ready_for_data.clear()
                self.datum = datum
                self.data_is_available.set()
        self.producer_thread = threading.Thread(target=producer_fn)

    def run(self):
        self.producer_thread.start()
        try:
            while True:
                self.data_is_available.wait()
                self.data_is_available.clear()
                if self.done:
                    break
                datum = self.datum
                self.ready_for_data.set()
                self.consume(datum)
        except KeyboardInterrupt:
            self.ready_for_data.set()
            self.producer_thread.join()
            raise
        self.producer_thread.join()

    @abc.abstractmethod
    def produce(self):
        "Either return a datum or raise NoMoreData."

    @abc.abstractmethod
    def consume(self, datum):
        "Use the given datum."
