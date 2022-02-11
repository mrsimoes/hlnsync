#!/usr/bin/python3

# Copyright (C) 2020 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
        assert main_thread in ("consumer", "producer"), \
            "ProducerConsumerThreaded.__init__ unexpected thread id"
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
    threads = set()
    try:
        for obj in objs:
            fn = lambda : fn_task(obj)
            new_thread = threading.Thread(target=fn)
            threads.add(new_thread)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        pass



#def thread_executor_terminator(fn_task, objs, worth_threading):
#    if not objs:
#        return
#    if len(objs) == 1:
#        fn_task(objs[0])
#        return
#    if not worth_threading:
#        for obj in objs:
#            fn_task(obj)
#        return
#    with ThreadPoolExecutor(max_workers=len(objs)-1) as executor:
#        try:
##            sleep_time = 0.01
#            futures = [executor.submit(fn_task, obj) for obj in objs[:-1]]
#            fn_task(objs[-1])
#            executor.shutdown(wait=True)
##            while not all(future.done() for future in futures):
##                time.sleep(sleep_time)
##                sleep_time = min(0.25, sleep_time*2)
#        except (KeyboardInterrupt, Exception):
#            executor.shutdown()
##            breakpoint()
##            executor._threads.clear()
##            concurrent.futures.thread._threads_queues.clear()
#            raise