#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
FileHashTree inherits from FilePropertyTree and manages a database of file hash
values.

__init accepts new size_as_hash kw argument.
"""

from __future__ import print_function

import sys

import lnsync_pkg.printutils as pr
from lnsync_pkg.blockhash import hash_file
from lnsync_pkg.onlineoffline import OnOffObject
from lnsync_pkg.proptree import FilePropTree, PropDBManager

class EmptyDBManager(OnOffObject):
    _onoff_super = PropDBManager
    def __enter__():
        pass
    def __exit__(*args):
        return False

class FileHashTree(OnOffObject):
    _onoff_super = FilePropTree

    def __init__(self, **kwargs):
        size_as_hash = kwargs.get("size_as_hash", False)
        self._size_as_hash = size_as_hash
        if size_as_hash:
            self.get_prop = self._get_prop_as_size
        super(FileHashTree, self).__init__(**kwargs)

    def set_dbmanager(self, db):
        super(FileHashTree, self).set_dbmanager(db)
        if db.mode == "offline" or  not self._size_as_hash:
            self._print_progress = "%s [%s]" % (self.db.dbpath, db.mode)
        else:
            self._print_progress = None

    def __enter__(self):
        """Open a database only in online mode and when not using s"""
        assert self.db
        if self._print_progress is not None:
            pr.info("opening %s" % (self._print_progress,))
        return super(FileHashTree, self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db is not None:
            pr.info("closing %s" % (self._print_progress,))
        return super(FileHashTree, self).__exit__(exc_type, exc_val, exc_tb)

    def _get_prop_as_size(self, file_obj):
        # Bypass everything SQL-related and just return the file size obtained
        # from scanning the source file tree (disk or offline).
        return file_obj.file_metadata.size

    @classmethod
    def listof(cls, init_args_list):
        """Return a context manager that will create/destroy a list of classref
        initialized by the given list of kwargs.
        TODO this belongs in PropTree
        """
        return ListContextManager(cls, init_args_list)


class FileHashTreeOnline(FileHashTree):
    def prop_from_source(self, file_obj):
        """Recompute and return prop for source file at relpath. (Online mode
        only.)
        """
        relpath = file_obj.relpaths[0]
        abspath = self.rel_to_abs(relpath)
        pr.progress("hashing %s" % self.printable_path(relpath))
        val = hash_file(abspath)
        assert isinstance(val, (int, long)), \
            "prop_from_source: bad property value %s" % (val,)
        return val

class ListContextManager(object):
    """Manage a context that creates a list of classref instances
    initialized from a given list of init_args dicts.
    TODO belongs in PropTree
    """
    def __init__(self, classref, init_args_list):
        self.classref = classref
        self.init_args_list = init_args_list
        self.objs_entered = []

    def __enter__(self):
        try:
            for kwargs in self.init_args_list:
                obj = self.classref(**kwargs)
                obj.__enter__()
                self.objs_entered.append(obj)
        except Exception as exc:
            traceback = sys.exc_info()[2]
            if not self.__exit__(type(exc), exc, traceback):
                raise type(exc), exc, traceback
        return self.objs_entered

    def __exit__(self, exc_type, exc_value, traceback):
        res = False # By default, the exception was not handled here.
        for obj in reversed(self.objs_entered):
            res = res or obj.__exit__(exc_type, exc_value, traceback)
        return res
