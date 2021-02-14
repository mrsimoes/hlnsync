#!/usr/bin/env python

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
from lnsync_pkg.fstr_type import fstr2str, fstr
import lnsync_pkg.printutils as pr
from lnsync_pkg.blockhash import hash_file
from lnsync_pkg.modaltype import onofftype, OFFLINE, ONLINE
from lnsync_pkg.proptree import \
    FilePropTree, PropDBManager, TreeError, \
    PropDBException, PropDBError, PropDBNoValue, \
    FileItemProp

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

    @abc.abstractmethod
    def prop_from_source(self, file_obj):
        pass

class NoSizeFileItem(FileItemProp):
    __slots__ = ()

    def __init__(self, file_id, metadata):
        super().__init__(file_id, metadata)
        self.file_metadata.size = 1

MAX_POS_INT = 2**63 - 1

class FileHashTreeOnline(FileHashTree, mode=ONLINE):
    def __init__(self, *args, hasher_exec=None, filter_exec=None, **kwargs):
        super().__init__(*args, **kwargs)
        if hasher_exec is not None:
            self._file_type = NoSizeFileItem
        self._hasher_exec = hasher_exec
        self._filter_exec = filter_exec

    def _prop_from_hasher_exec(self, abspath):
        try:
            procres = subprocess.run(
                [self._hasher_exec, fstr2str(abspath)],
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
        if res > MAX_POS_INT:
            res = MAX_POS_INT - res
        return res

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
