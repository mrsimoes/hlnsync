#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
FileHashDB inherits from FilePropertyDB and manages a database of file hash values.
"""

from __future__ import print_function

import lnsync_pkg.printutils as pr
from lnsync_pkg.blockhash import hash_file
from lnsync_pkg.propdb import FilePropertyDB, FilePropertyDBs, copy_db

class FileHashDB(FilePropertyDB):
    """A file hash value database.
    """

    def __init__(self, dbpath, mode, size_as_hash=False, maxsize=None):
        """Open/create hash database.

        If size_as_hash is True, use file size as the hash value.
        """
        self.size_as_hash = size_as_hash
        if size_as_hash:
            self.db_get_prop = self._get_prop_as_size
        super(FileHashDB, self).__init__(dbpath, mode, maxsize)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.sqlmanager is not None:
            pr.warning("closing %s [%s]" % (self.dbpath, self.mode))
        return super(FileHashDB, self).__exit__(exc_type, exc_val, exc_tb)

    def _get_prop_as_size(self, file_obj):
        return file_obj.file_metadata.size

    def _create_sql_manager(self, dbpath):
        pr.warning("opening %s [%s]" % (self.dbpath, self.mode))
        if self.mode == "offline" or not self.size_as_hash:
            super(FileHashDB, self)._create_sql_manager(dbpath)

    def prop_from_source(self, file_obj):
        """Recompute and return prop for source file at relpath. (Online mode only.)
        """
        relpath = file_obj.relpaths[0]
        abspath = self.rel_to_abs(relpath)
        pr.progress("hashing %s" % self.printable_path(relpath))
        val = hash_file(abspath)
        assert isinstance(val, (int, long)), "prop_from_source: bad property value %s" % (val,)
        return val

class FileHashDBs(FilePropertyDBs):
    """Another name for setting up a context for multiple HashDB.
    """
    pass
