#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Odds and ends.
"""

# pylint: disable=import-outside-toplevel

import os
import sys
import argparse
from functools import reduce

MAX_UINT64 = 2**64 - 1
MAX_INT64 = 2**63 - 1
MIN_INT64 = -2**63

def uint64_to_int64(value):
    value = int(value)
    if value <= MAX_INT64:
        res = value
    else:
        res = MAX_INT64 - value
    assert MIN_INT64 <= res <= MAX_INT64, \
        f"uint64_to_int64 overflow: {value}"
    return res

def int64_to_uint64(value):
    value = int(value)
    if value >= 0:
        res = value
    else:
        res = MAX_INT64 - value
    assert 0 <= res <= MAX_UINT64, \
        f"int64_to_uint64 overflow: {value}"
    return res

MAX_UINT32 = 2**32 - 1
MAX_INT32 = 2**31 - 1
MIN_INT32 = -2**31

def uint32_to_int32(value):
    if value <= MAX_INT64:
        res = value
    else:
        res = MAX_INT32 - value
    assert MIN_INT32 <= res <= MAX_INT32, \
        f"uint32_to_int32 overflow: {value}"
    return res

def int32_to_uint32(value):
    if value >= 0:
        res = value
    else:
        res = MAX_INT32 - value
    assert 0 <= res <= MAX_UINT32, \
        f"int32_to_uint32 overflow: {value}"
    return res


class HelperAppError(Exception):
    def __init__(self, cmd, error_msg):
        super().__init__()
        if isinstance(cmd, (list, tuple)):
            assert all(lambda x: isinstance(x, str), cmd)
            cmd = " ".join(cmd)
        self.cmd = cmd
        self.error_msg = error_msg
    def __str__(self):
        return "while running %s: %s" %(self.cmd, self.error_msg)


def wrap_text(text, width):
    """
    A word-wrap function that preserves existing line breaks
    and most spaces in the text. Expects that existing line
    breaks are posix newlines (\n).
    By Mike Brown, licensed under the PSF.
    """
    return reduce(lambda line, word, width=width: '%s%s%s' %
                  (line,
                   ' \n'[(len(line)-line.rfind('\n')-1
                          + len(word.split('\n', 1)[0]
                               ) >= width)],
                   word,),
                  text.split(' '),)

def set_exception_hook():
    def info(exc_type, value, traceback):
        if hasattr(sys, 'ps1') or not sys.stderr.isatty():
        # we are in interactive mode or we don't have a tty-like
        # device, so we call the default hook
            sys.__excepthook__(exc_type, value, traceback)
        else:
            v = sys.exc_info()
            import traceback
            import pdb
            # we are NOT in interactive mode, print the exception...
            traceback.print_exception(exc_type, value, traceback)
            # ...then start the debugger in post-mortem mode.
            # pdb.pm() # deprecated
            pdb.post_mortem(traceback) # more "modern"
    sys.excepthook = info

def is_subdir(subdir, topdir):
    """
    Test if subdir is topdir or a subdir of topdir.
    Return either False or the relative path (which is never an empty string).
    """
    assert os.path.isdir(subdir) and os.path.isdir(topdir), \
        f"is_subdir: expected dirs, got: '{subdir}' and '{topdir}'"
    relative = os.path.relpath(subdir, topdir) # (path, start)
    if relative.startswith(os.pardir + os.sep):
        return False
    else:
        return relative

def is_subdir_strict(subdir, topdir):
    return is_subdir(subdir, topdir) and not os.path.samefile(subdir, topdir)

class ListContextManager:
    """
    A context manager that enters a sequence of classref instance context
    managers, initialized from a given list of init_args dictionaries,
    and exits them in reverse.
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

class StoreBoolAction(argparse.Action):
    """
    argparse.Action that store true by default and stores false if option switch
    starts with '--no-'.
    """
    def __init__(self, *args, nargs=0, **kwargs):
        super().__init__(*args, nargs=0, **kwargs)
    def __call__(self, parser, namespace, pos_arg, option_string=None):
        if option_string and option_string[0:5] == "--no-":
            val = False
        else:
            val = True
        setattr(namespace, self.dest, val)

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

def append_to_namespace_list(namespace, key, more_items):
    prev_items = getattr(namespace, key, None)
    if prev_items is None: # It could have been None before.
        prev_items = []
    setattr(namespace, key, prev_items + more_items)
