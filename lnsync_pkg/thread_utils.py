#!/usr/bin/env python

# Copyright (C) 2020 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Included:

An abstract class that provides producer-consumer synchronized threading with a
buffer size of one datum.

A thread pool creator that terminates all threads on SIGINT.
"""

# pylint: disable=protected-access

import time
import abc

import threading
import concurrent.futures.thread
from concurrent.futures import ThreadPoolExecutor

class NoMoreData(Exception):
    pass

class ProducerConsumerThreaded:
    """
    Coordinate data Producer and Consumer threads.
    """

    def __init__(self, main_thread="producer"):
        """
        main_thread = either "consumer" or "producer".
        Only the main thread gets the KeyboardInterrupt SIGINT signal.
        """
        assert main_thread in ("consumer", "producer")
        self.ready_for_data = threading.Event()
        self.ready_for_data.set()
        self.data_is_available = threading.Event() # Set also at NoMoreData.
        self.done = False
        self.datum = None
        if main_thread == "consumer":
            self._primary_loop = self._consumer_loop
            self._secondary_loop = self._producer_loop
        else:
            self._primary_loop = self._producer_loop
            self._secondary_loop = self._consumer_loop
        self._secondary_thread = threading.Thread(target=self._secondary_loop)

    def run(self):
        self._secondary_thread.start()
        try:
            self._primary_loop()
        finally:
            self._secondary_thread.join()

    def _producer_loop(self):
        try:
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
                if self.done:
                    break
                self.datum = datum
                self.data_is_available.set()
        except KeyboardInterrupt:
            self.done = True
            self.data_is_available.set()
            raise

    def _consumer_loop(self):
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
            self.done = True
            self.ready_for_data.set()
            raise

    @abc.abstractmethod
    def produce(self):
        """
        Either return a datum or raise NoMoreData.
        """

    @abc.abstractmethod
    def consume(self, datum):
        """
        Use the given datum.
        """


def thread_executor_terminator(fn_task, objs, worth_threading):
    if not objs:
        return
    if len(objs) == 1:
        fn_task(objs[0])
        return
    if not worth_threading:
        for obj in objs:
            fn_task(obj)
        return
    with ThreadPoolExecutor(max_workers=len(objs)) as executor:
        try:
            sleep_time = 0.01
            futures = [executor.submit(fn_task, obj) for obj in objs[:-1]]
            fn_task(objs[-1])
            while not all(future.done() for future in futures):
                time.sleep(sleep_time)
                sleep_time = min(0.25, sleep_time*2)
        except (KeyboardInterrupt, Exception):
            executor._threads.clear()
            concurrent.futures.thread._threads_queues.clear()
            raise
