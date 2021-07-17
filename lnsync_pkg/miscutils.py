#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Odds and ends.
"""

import os

MAX_INT64 = 1<<63 - 1

def uint64_to_int64(value):
    if value <= MAX_INT64:
        return value
    else:
        return MAX_INT64 - value

def int64_to_uint64(value):
    if value >= 0:
        return value
    else:
        return MAX_INT64 - value

MAX_INT32 = 1<<31 - 1

def uint32_to_int32(value):
    if value <= MAX_INT32:
        return value
    else:
        return MAX_INT32 - value

def int32_to_uint32(value):
    if value >= 0:
        return value
    else:
        return MAX_INT32 - value


def is_subdir(subdir, topdir):
    """
    Test if subdir is topdir or in topdir.
    Return either False or the relative path (which is never an empty string).
    """
    relative = os.path.relpath(subdir, topdir) # (path, start)
    if relative.startswith(os.pardir + os.sep):
        return None
    else:
        return relative

class ListContextManager:
    """
    Manage a context that creates a list of classref instances
    initialized from a given list of init_args dicts.
    """
    def __init__(self, classref, init_args_list):
        self.classref = classref
        self.init_args_list = init_args_list
        self.objs_entered = []

    def __enter__(self):
        for kwargs in self.init_args_list:
            obj = self.classref(**kwargs)
            val = obj.__enter__()
            self.objs_entered.append(val)
        return self.objs_entered

    def __exit__(self, exc_type, exc_value, traceback):
        res = False # By default, the exception is not suppressed.
        for obj in reversed(self.objs_entered):
            res = res or obj.__exit__(exc_type, exc_value, traceback)
        return res

class BitField:
    """
    Based on
    https://code.activestate.com/recipes/113799-bit-field-manipulation/
    Licensed under PSF.
    """
    def __init__(self, value=0):
        self._d = value

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.__getslice__(index.start, index.stop)
        else:
            return (self._d >> index) & 1

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            self.__setslice__(index.start, index.stop, value)
        else:
            value = (value & 1) << index
            mask = 1 <<index
            self._d = (self._d & ~mask) | value

    def __getslice__(self, start, end):
        mask = 2**(end - start) -1
        return (self._d >> start) & mask

    def __setslice__(self, start, end, value):
        mask = 2**(end - start) -1
        value = (value & mask) << start
        mask = mask << start
        self._d = (self._d & ~mask) | value
        return (self._d >> start) & mask

    def __int__(self):
        return self._d
