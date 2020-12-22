#!/usr/bin/env python

# Copyright (C) 2020 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Included:

An abstract class that provides producer-consumer synchronized threading with a
buffer size of one datum.

A thread pool creator that terminates all threads on SIGINT.
"""

import time
import abc

import threading
import concurrent.futures.thread
from concurrent.futures import ThreadPoolExecutor, as_completed

class NoMoreData(Exception):
    pass

class ProducerConsumerThreaded:
    """
    Coordinate data Producer and Consumer threads.
    """
    def __init__(self):
        self.ready_for_data = threading.Event()
        self.ready_for_data.set()
        self.data_is_available = threading.Event() # Set also at NoMoreData.
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


def thread_executor_terminator(fn, objs):
    with ThreadPoolExecutor(max_workers=len(objs)) as executor:
        try:
            futures = [executor.submit(fn, obj) for obj in objs]
            while not all(future.done() for future in futures):
                time.sleep(0.2)
        except KeyboardInterrupt:
            executor._threads.clear()
            concurrent.futures.thread._threads_queues.clear()
            raise
