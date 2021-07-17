#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Uniquely specify a database file at dirpath by file prefix.
"""

import os
import random
import re

DEFAULT_DBPREFIX = "lnsync-"

def mode_from_location(location, mandatory_mode=None):
    """
    Given a location path for a file tree, decide if it is an offline
    tree (location is a file) or a an online tree (location is a dir).
    Raise ValueError if location is neither a file or a dir, or if the
    location is not readable.
    """
    if mandatory_mode is None:
        if os.path.isdir(location):
            mandatory_mode = "online"
        elif os.path.isfile(location):
            mandatory_mode = "offline"
        else:
            msg = "expected file or dir: " + location
            raise ValueError(msg)
    if mandatory_mode == "online":
        if not os.path.isdir(location):
            msg = "expected tree root dir: " + location
            raise ValueError(msg)
        elif not os.access(location, os.R_OK):
            msg = "cannot read from: " + location
            raise ValueError(msg)
        mode = "online"
    elif mandatory_mode == "offline":
        if os.path.exists(location):
            if not os.path.isfile(location):
                msg = "expected offline tree file: " + location
                raise ValueError(msg)
            elif not os.access(location, os.R_OK):
                msg = "cannot read from: " + location
                raise ValueError(msg)
        mode = "offline"
    return mode

def pick_db_basename(dir_path, dbprefix=DEFAULT_DBPREFIX):
    """
    Find or create a unique basename matching <dbprefix>[0-9]*.db in the
    directory. Raise EnvironmentError if there are too many files matching the
    database basename pattern or if there are none and the given dir is not
    writable.
    Raise EnvironmentError if too many db files or no write access.
    """
    assert os.path.isdir(dir_path), \
            "pick_db_basename: not a directory: " + dir_path
    if dbprefix.endswith(".db"):
        dbprefix = dbprefix[:-3]
    pattern = "^%s[0-9]*.db$" % (dbprefix,)
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
        db_basename = dbprefix + random_digit_str() + ".db"
    else:
        raise EnvironmentError("too many db files at " + dir_path)
    return db_basename
