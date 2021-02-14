#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Odds and ends.
"""

import os
from lnsync_pkg.fstr_type import fstr

def is_subdir(subdir, topdir):
    """
    Test if subdir is topdir or in topdir.
    Return either False or the relative path (which is never an empty string).
    """
    relative = os.path.relpath(subdir, topdir) # (path, start)
    if relative.startswith(fstr(os.pardir + os.sep)):
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
