#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Define the base class and a number of exceptions for property DB managers.
"""

# pylint: disable=unused-import

from lnsync_pkg.modaltype import onofftype, Mode

class PropDBException(Exception):
    pass

class PropDBError(PropDBException):
    pass

class PropDBNoValue(PropDBException):
    pass

class PropDBStaleValue(PropDBException):
    pass

class PropDBManager(metaclass=onofftype):
    def get_glob_patterns(self):
        return []

    def __init__(self, *args, **kwargs):
        pass
