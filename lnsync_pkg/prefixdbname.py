#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Uniquely specify a database file at dirpath by file prefix.
"""

import os
import fnmatch
import random

from lnsync_pkg.p23compat import fstr, fstr2str
from lnsync_pkg.sqlpropdb import SQLPropDBManager

def mk_online_db(dir_path, db_basename):
    return SQLPropDBManager(os.path.join(dir_path, db_basename), mode="online")

def mode_from_location(location, mandatory_mode=None):
    """Given a location path for a file tree, decide if it is an offline
    tree (location is a file) or a an online tree (location is a dir).
    Raise ValueError if location is neither a file or a dir, or if the
    location is not readable."""
    if mandatory_mode is None:
        if os.path.isdir(location):
            mandatory_mode = "online"
        elif os.path.isfile(location):
            mandatory_mode = "offline"
        else:
            msg = "expected file or dir: %s" % (fstr2str(location),)
            raise ValueError(msg)
    if mandatory_mode == "online":
        if not os.path.isdir(location):
            msg = "expected tree root dir: %s" % (fstr2str(location),)
            raise ValueError(msg)
        elif not os.access(location, os.R_OK):
            msg = "cannot read from: %s" % (fstr2str(location),)
            raise ValueError(msg)
        mode = "online"
    elif mandatory_mode == "offline":
        if os.path.exists(location):
            if not os.path.isfile(location):
                msg = "expected offline tree file: %s" % (fstr2str(location),)
                raise ValueError(msg)
            elif not os.access(location, os.R_OK):
                msg = "cannot read from: %s" % (fstr2str(location),)
                raise ValueError(msg)
        mode = "offline"
    return mode

DB_PREFIX = "prop-"

def set_prefix(prefix):
    global DB_PREFIX
    DB_PREFIX = prefix

def pick_db_basename(dir_path, dbprefix):
    """Find or create a unique basename matching <dbprefix>[0-9]*.db in the
    directory. Raise EnvironmentError if there are too many files matching the
    database basename pattern or if there are none and the given dir is not
    writable.
    dir_path should be fstr type.
    Raise EnvironmentError if too many db files or no write access.
    """
    assert os.path.isdir(dir_path), \
            "pick_db_basename: not a directory: %s" % fstr2str(dir_path)
    if dbprefix.endswith(fstr(".db")):
        dbprefix = dbprefix[:-3]
    pattern = fstr("%s[0-9]*.db" % fstr2str(dbprefix))
    candidates_base = fnmatch.filter(os.listdir(dir_path), pattern)
    if len(candidates_base) == 1:
        db_basename = candidates_base[0]
    elif candidates_base == []:
        if not os.access(dir_path, os.W_OK):
            msg = "no write access to %s" % fstr2str(dir_path)
            raise EnvironmentError(msg)
        def random_digit_str(ndigit=3):
            """Return a random string of digits of length ndigit."""
            return ("%%0%dd" % ndigit) % random.randint(0, 10**ndigit-1)
        db_basename = dbprefix + fstr(random_digit_str()) + fstr(".db")
    else:
        raise EnvironmentError("too many db files at %s" % fstr2str(dir_path))
    return db_basename
