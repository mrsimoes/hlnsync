#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
FileHashTree inherits from FilePropertyTree and uses the provided dbmanager to
store and retrieve hash values.

The basic mode of operation is to compute the quick xxhash on file contents
when required. The computation effort is essentially the same as reading the
file.

If a custom hasher is provided, then set size always to 1.

#TODO Implement filter with e.g. FIFO.
"""

# pylint: disable=unused-import, no-self-use

import subprocess

import abc
import lnsync_pkg.printutils as pr
from lnsync_pkg.miscutils import uint64_to_int64
from lnsync_pkg.hasher_functions import HasherManager
from lnsync_pkg.modaltype import onofftype, Mode
from lnsync_pkg.propdbmanager import PropDBManager, PropDBException, \
    PropDBError, PropDBNoValue
from lnsync_pkg.proptree import FilePropTree, TreeError, FileItemProp
from lnsync_pkg.sqlpropdb import SQLPropDBManager

class FileHashTree(FilePropTree, metaclass=onofftype):
    def __init__(self, **kwargs):
        """
        kwargs passed upwards to FilePropTree:
            - dbmaker, a callable that accepts kwargs dbpath, and root.
                If dbmaker is not None, it is called with root, mode and
                **dbkwargs to return a database manager object.
                (This delays connecting to the db as much as possible.)
            - dbkwargs, args to be fed to the previous factory.
            - topdir_path: path disk file tree (may be None if FileTree is
                somehow virtual).
            - exclude_pattern: None or a list of glob patterns of
                relative paths to ignore when reading from disk.
            - use_metadata: if True read metadata index files by size
            - maxsize: ignore files larger than this, is positive and not None
            - skipempty: ignore zero-length files
            - writeback: if True, path operation methods update the disk tree.
            - file_type, dir_type: classes to instantiate.
        """
        dbmaker = kwargs.pop("dbmaker", SQLPropDBManager)
        size_as_hash = kwargs.get("size_as_hash", False)
        self._size_as_hash = size_as_hash
        if size_as_hash:
            self.get_prop = self._get_prop_as_size
            self.db_get_uptodate_prop = self._db_get_uptodate_prop_as_size
        super(FileHashTree, self).__init__(dbmaker=dbmaker, **kwargs)
        self._print_progress = None

    def _get_prop_as_size(self, file_obj):
        # Bypass everything SQL-related and just return the file size obtained
        # from scanning the source file tree (disk or offline).
        return file_obj.file_metadata.size

    def _db_get_uptodate_prop_as_size(self, file_obj, delete_stale):
        breakpoint()

    def set_dbmanager(self, db):
        super(FileHashTree, self).set_dbmanager(db)
        if db.mode == Mode.OFFLINE or not self._size_as_hash:
            self._print_progress = \
                "%s [%s]" % (self.db.dbpath, db.mode)

    def __enter__(self):
        assert self.db, \
            "__enter__: missing db"
        if self._print_progress is not None:
            pr.info("opening %s" % (self._print_progress,))
        return super(FileHashTree, self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._print_progress is not None:
            pr.info("closing %s" % (self._print_progress,))
        return super(FileHashTree, self).__exit__(exc_type, exc_val, exc_tb)

    @abc.abstractmethod
    def prop_from_source(self, file_obj):
        pass

class FileHashTreeOnline(FileHashTree, mode=Mode.ONLINE):
    def prop_from_source(self, file_obj):
        """
        Recompute and return prop for source file at relpath (online mode only).
        Raise RuntimeError if something goes wrong.
        """
        relpath = file_obj.relpaths[0]
        abspath = self.rel_to_abs(relpath)
        pr.progress("hashing:%s" % self.printable_path(relpath))
        try:
            val = HasherManager.get_hasher().hash_file(abspath)
        except Exception as exc: # TODO tighten this.
            raise TreeError(
                f"while hashing: {str(exc)}",
                file_obj=file_obj,
                tree=self) from exc
        if not isinstance(val, int):
            raise  RuntimeError("bad property value %s" % (val,))
        return val
