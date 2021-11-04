#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Uniquely specify a database file at dirpath by file prefix.

Database filenames match <PREFIX>-[0-9]*.db
"""

# pylint: global-statement

import os
import random
import re

from lnsync_pkg.modaltype import Mode

_DEFAULT_DEFAULT_DBPREFIX = "lnsync"

_DEFAULT_DBPREFIX = _DEFAULT_DEFAULT_DBPREFIX

def get_default_dbprefix():
    return _DEFAULT_DBPREFIX

def set_default_dbprefix(new_prefix):
    global _DEFAULT_DBPREFIX
    _DEFAULT_DBPREFIX = new_prefix

def adjust_default_dbprefix(to_add):
    set_default_dbprefix(_DEFAULT_DEFAULT_DBPREFIX + "-" + to_add)

def mode_from_location(location, mandatory_mode=Mode.NONE):
    """
    Given a location path for a file tree, decide if it is an offline
    tree (location is a file) or a an online tree (location is a dir).
    Raise ValueError if location is neither a file or a dir, or if the
    location is not readable.
    """
    assert isinstance(mandatory_mode, Mode), \
        f"mode_from_location: not a mode: {mandatory_mode}"
    if mandatory_mode is Mode.NONE:
        if os.path.isdir(location):
            mandatory_mode = Mode.ONLINE
        elif os.path.isfile(location):
            mandatory_mode = Mode.OFFLINE
        else:
            msg = "expected file or dir: " + location
            raise ValueError(msg)
    if mandatory_mode == Mode.ONLINE:
        if not os.path.isdir(location):
            msg = "expected tree root dir: " + location
            raise ValueError(msg)
        elif not os.access(location, os.R_OK):
            msg = "cannot read from: " + location
            raise ValueError(msg)
        mode = Mode.ONLINE
    elif mandatory_mode == Mode.OFFLINE:
        if os.path.exists(location):
            if not os.path.isfile(location):
                msg = "expected offline tree file: " + location
                raise ValueError(msg)
            elif not os.access(location, os.R_OK):
                msg = "cannot read from: " + location
                raise ValueError(msg)
        mode = Mode.OFFLINE
    return mode

def pick_db_basename(dir_path, dbprefix=None):
    """
    Find or create a unique basename matching <dbprefix>-[0-9]+.db in the
    directory. Raise EnvironmentError if there are too many files matching the
    database basename pattern or if there are none and the given dir is not
    writable.
    Raise EnvironmentError if too many db files or no write access.
    """
    if dbprefix is None:
        dbprefix = get_default_dbprefix()
    assert os.path.isdir(dir_path), \
            "pick_db_basename: not a directory: " + dir_path
    pattern = r"^%s-\d+.db$" % (dbprefix,)
    regex = re.compile(pattern)
    candidates_base = []
    for basename in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, basename)) \
                and regex.match(basename):
            candidates_base.append(basename)
    if len(candidates_base) == 1:
        db_basename = candidates_base[0]
    elif candidates_base == []:
        if not os.access(dir_path, os.W_OK):
            msg = "no write access to " + dir_path
            raise EnvironmentError(msg)
        def random_digit_str(ndigit=3):
            """Return a random string of digits of length ndigit."""
            return ("%%0%dd" % ndigit) % random.randint(0, 10**ndigit-1)
        db_basename = dbprefix + "-" + random_digit_str() + ".db"
    else:
        raise EnvironmentError("too many db files at " + dir_path)
    return db_basename
