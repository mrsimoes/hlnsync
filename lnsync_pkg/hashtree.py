#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
FileHashTree inherits from FilePropertyTree and manages a database of file hash
values.

__init accepts new size_as_hash kw argument.
"""


from lnsync_pkg.p23compat import fstr2str
import lnsync_pkg.printutils as pr
from lnsync_pkg.blockhash import hash_file
from lnsync_pkg.modaltype import onofftype, OFFLINE, ONLINE
from lnsync_pkg.proptree import \
    FilePropTree, PropDBManager, TreeError, \
    PropDBException, PropDBError, PropDBNoValue

class EmptyDBManager(PropDBManager, metaclass=onofftype):
    def __enter__():
        pass

    def __exit__(*args):
        return False

class FileHashTree(FilePropTree, metaclass=onofftype):
    def __init__(self, **kwargs):
        size_as_hash = kwargs.get("size_as_hash", False)
        self._size_as_hash = size_as_hash
        if size_as_hash:
            self.get_prop = self._get_prop_as_size
        super(FileHashTree, self).__init__(**kwargs)
        self._print_progress = None

    def set_dbmanager(self, db):
        super(FileHashTree, self).set_dbmanager(db)
        if db.mode == OFFLINE or not self._size_as_hash:
            self._print_progress = \
                "%s [%s]" % (fstr2str(self.db.dbpath), db.mode)

    def __enter__(self):
        assert self.db
        if self._print_progress is not None:
            pr.info("opening %s" % (self._print_progress,))
        return super(FileHashTree, self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._print_progress is not None:
            pr.info("closing %s" % (self._print_progress,))
        return super(FileHashTree, self).__exit__(exc_type, exc_val, exc_tb)

    def _get_prop_as_size(self, file_obj):
        # Bypass everything SQL-related and just return the file size obtained
        # from scanning the source file tree (disk or offline).
        return file_obj.file_metadata.size


class FileHashTreeOnline(FileHashTree, mode=ONLINE):
    def prop_from_source(self, file_obj):
        """
        Recompute and return prop for source file at relpath (online mode only).
        Raise RuntimeError if something goes wrong.
        """
        relpath = file_obj.relpaths[0]
        abspath = self.rel_to_abs(relpath)
        pr.progress("hashing:%s" % self.printable_path(relpath))
        try:
            val = hash_file(abspath)
        except OSError as exc:
            raise RuntimeError("while hashing") from exc
        if not isinstance(val, int):
            raise  RuntimeError("bad property value %s" % (val,))
        return val

