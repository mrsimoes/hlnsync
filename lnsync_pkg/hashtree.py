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
from lnsync_pkg.blockhash import BlockHasher, hash_file
from lnsync_pkg.modaltype import onofftype, OFFLINE, ONLINE
from lnsync_pkg.propdbmanager import PropDBManager, PropDBException, PropDBError, PropDBNoValue
from lnsync_pkg.proptree import FilePropTree, TreeError, FileItemProp
from lnsync_pkg.sqlpropdb import SQLPropDBManager

class FileHashTree(FilePropTree, metaclass=onofftype):
    def __init__(self, filter_exec=None, **kwargs):
        """
        kwargs passed upwards to FilePropTree:
            - dbmaker, a callable that accepts kwargs dbpath, and root.
                If dbmaker is not None, it is called with root, mode and
                **dbkwargs to return a database manager object.
                (This delays connecting to the db as much as possible.)
            - dbkwargs, args to be fed to the previous factory.
            - topdir_path: path disk file tree (may be None if FileTree is somehow
                virtual).
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
# TODO
#        if "dbkwargs" not in kwargs:
#            kwargs["dbkwargs"] = {}
#        if hasher_exec is not None:
#            kwargs["dbkwargs"]["hasher"] = blockhash.Hashers.CUSTOM
#        else:
#            kwargs["dbkwargs"]["hasher"] = blockhash.get_hasher()
        super(FileHashTree, self).__init__(dbmaker=dbmaker, **kwargs)
        self._print_progress = None

    def set_dbmanager(self, db):
        super(FileHashTree, self).set_dbmanager(db)
        if db.mode == OFFLINE or not self._size_as_hash:
            self._print_progress = \
                "%s [%s]" % (self.db.dbpath, db.mode)

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

    @abc.abstractmethod
    def prop_from_source(self, file_obj):
        pass

class NoSizeFileItem(FileItemProp):
    __slots__ = ()

    def __init__(self, file_id, metadata):
        super().__init__(file_id, metadata)
        self.file_metadata.size = 1

class FileHashTreeOnline(FileHashTree, mode=ONLINE):
    def __init__(self, *args, hasher_exec=None, filter_exec=None, **kwargs):
        super().__init__(*args, hasher_exec=hasher_exec, filter_exec=filter_exec, **kwargs)
        if hasher_exec is not None:
            self._file_type = NoSizeFileItem
        self._hasher_exec = hasher_exec
        self._filter_exec = filter_exec

    def _prop_from_hasher_exec(self, abspath):
        try:
            procres = subprocess.run(
                [self._hasher_exec, abspath],
                capture_output=True, check=True)
        except subprocess.CalledProcessError as exc:
            msg = "failed hashing: %s (%s)." % (abspath, procres.stderr, )
            raise RuntimeError(msg) from exc
        try:
            res = int(procres.stdout)
        except ValueError as msg:
            msg = "invalid hasher output for %s (%s)." % \
                    (abspath, procres.stdout)
            raise RuntimeError(msg) from exc
        return uint64_to_int64(res)

    def prop_from_source(self, file_obj):
        """
        Recompute and return prop for source file at relpath (online mode only).
        Raise RuntimeError if something goes wrong.
        """
        relpath = file_obj.relpaths[0]
        abspath = self.rel_to_abs(relpath)
        pr.progress("hashing:%s" % self.printable_path(relpath))
        if not self._hasher_exec:
            try:
                val = hash_file(abspath, filter_exec=self._filter_exec)
            except OSError as exc:
                raise RuntimeError("while hashing") from exc
        else:
            val = self._prop_from_hasher_exec(abspath)
        if not isinstance(val, int):
            raise  RuntimeError("bad property value %s" % (val,))
        return val
