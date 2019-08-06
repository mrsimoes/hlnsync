#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Provides a framework for persistent caching store of an integer-valued
property value associated to each file in a file tree.

On request, the property valued is read from persistent database cache,
if it has been stored and the file hasn't been modified since then
(by metadata stamp, i.e. size and mtime). Otherwise, the property valued is
recomputed and stored in the persistent cache.

There are two modes of operation: "online" and "offline".

- In "online" mode, access to the disk file tree is needed and the database
may be updated. In this mode, root_path must be a dir, the root of the disk
file tree.

- In "offline" mode, all property values as well as the tree data structure is
stored in the database. Access to the original source file tree is unneeded.
In this mode, root_path is set to None.

To use, implement online/offline alternative hierarchies for subclasses of
PropDBManager and FilePropTree using the protocol described in onlineoffline.py.

The database top class should be given by a kw argument "db", a callable
that takes a location, an optional mode (default "online") and optional
kwargs and returns the appropriate db instance (e.g. the client subclass).

The db instance is created at the end of tree init process, but before any tree
scanning is done.

Alternatively, the appropriate db object may be set after the init process and
before scanning by calling tree.set_dbmanager(dbobj)

In either case, the db instance is given an opportunity to exclude files and
dirs from being scanned.

An example SQLite3 database manager is provided in sqldbmanager.py.

A context must always be used, to properly open and close the database.
    with tree_obj:
        <body>

Create multiple trees at once using:
    with MyPropDB.listof( [list of MyPropDB init kw args] ):
        <body>
"""

from __future__ import with_statement, print_function

import os
import abc

from six import iteritems, raise_from

from lnsync_pkg.p23compat import fstr2str

import lnsync_pkg.printutils as pr
from lnsync_pkg.fileid import make_id_computer
from lnsync_pkg.onlineoffline import OnOffObject
from lnsync_pkg.filetree import FileTree, FileItem, DirItem, OtherItem, TreeError

class PropDBError(Exception):
    pass

class PropDBValueError(PropDBError):
    pass

class PropDBManager(OnOffObject):
    _onoff_super = OnOffObject
    def get_exclude_patterns(self):
        return []
    def __init__(self, *args, **kwargs):
        pass

class FileItemProp(FileItem):
    """A file tree object that records a file property value along with a
    metadata stamp, ie file size, mtime and ctime.
    """
    __slots__ = "prop_value", "prop_metadata"
    def __init__(self, obj_id, metadata):
        super(FileItemProp, self).__init__(obj_id, metadata)
        self.prop_value = None
        self.prop_metadata = None

class FilePropTree(OnOffObject):
    """A file tree providing persistent cache of values for a file property.
    """
    _onoff_super = FileTree

    def __init__(self, **kwargs):
        """Takes kwargs mode (as OnOffObject object), root_path (as a FileTree)
        plus dbmaker, a callable that accepts kw args dbpath, and root.
        If dbmaker is not none, it is called with root, mode and **dbkwargs to
        return a database manager object.
        (This delays connecting to the db as much as possible.)"""
        self._enter_count = 0 # Context manager entry count.
        self.db = None
        mode = kwargs.get("mode", "online") # Default is online.
        assert mode in ("online", "offline")
        root_path = kwargs.pop("root_path")
        if mode == "online":
            assert os.path.isdir(root_path)
        else:
            assert root_path is None
        use_metadata = kwargs.pop("use_metadata", True)
        assert use_metadata
        super(FilePropTree, self).__init__(
            root_path=root_path,
            use_metadata=use_metadata,
            file_type=FileItemProp,
            **kwargs)
        dbmaker = kwargs.get("dbmaker")
        if dbmaker:
            assert callable(dbmaker)
            dbobj = dbmaker(
                mode=self.mode, **kwargs.get("dbkwargs", {}))
            self.set_dbmanager(dbobj)

    def set_dbmanager(self, dbmanager):
        assert self.mode == dbmanager.mode
        self.db = dbmanager
        self.add_exclude_patterns(dbmanager.get_exclude_patterns())
        if self.mode == "online":
            dbdir = os.path.dirname(dbmanager.dbpath)
            if  dbdir != self.root_path:
                self._id_computer = make_id_computer(dbdir)

    @abc.abstractmethod
    def prop_from_source(self, file_obj):
        """Should obtain file property value from tree source.
        Raise RuntimeError if something goes wrong."""
        pass

    def __enter__(self):
        """Create the SQL db manager, scan root directory."""
        self._enter_count += 1
        if self._enter_count == 1: # First entered.
            self.db.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._enter_count -= 1
        res = False
        if self._enter_count == 0:
            res = res or self.db.__exit__(exc_type, exc_val, exc_tb)
        return res # Do not suppress any exception.

    def get_prop(self, f_obj):
        """Return a file property value, from db if possible, updating db if
        needed. Raise TreeError if the value could not be obtained from
        database or from source.
        """
        assert f_obj is not None, "get_prop: no f_obj"
        if f_obj.prop_value is not None:
            return f_obj.prop_value
        res = self.db_get_uptodate_prop(f_obj, delete_stale=False)
        if res is not None:
            f_obj.prop_value = res
        return res

    def db_get_uptodate_prop(self, f_obj, delete_stale):
        """Return either a property value, if current, or None if the prop
        value is either not in the db, or its metadata stamp (db mtime + size)
        does not match the file metadata.
        If the prop value is stale, delete the stale entry from the db.
        May raise TreeError. """
        res = self.db_get_stored_prop(f_obj)
        if res is None:
            return None
        prop_val, prop_md = res
        if prop_md == f_obj.file_metadata:
            f_obj.prop_value = prop_val
            f_obj.prop_metadata = prop_md
            return prop_val
        else:
            pr.debug(
                "file %s md (id %d) changed from %s to %s, update_db: %s",
                fstr2str(f_obj.relpaths[0]), f_obj.file_id,
                f_obj.prop_metadata, f_obj.file_metadata,
                str(delete_stale))
            if delete_stale:
                self.db.delete_ids(f_obj.file_id)
            return None

    def db_get_stored_prop(self, f_obj):
        """Return stored prop value along with property value metadata stamp,
        as a pair, even if not up-to-date.
        May raise TreeError. """
        try:
            res = self.db.get_prop_metadata(f_obj.file_id)
        except PropDBError as exc:
            msg = "reading db for id %d: %s" % (f_obj.file_id, str(exc))
            pr.error(msg)
            raise_from(TreeError(msg), exc)
        return res

class FilePropTreeOffline(FilePropTree):
    """The file tree is stored in the property database along with up-to-date
    property values.
    """
    def db_get_uptodate_prop(self, f_obj, delete_stale):
        """Return either a property value, if current, or None if the prop
        value is either not in the db, or its db mtime stamp does not match
        the file mtime.
        If the prop value is stale, delete the stale entry from the db.
        May raise TreeError."""
        res = super(FilePropTreeOffline, self).db_get_uptodate_prop(
            f_obj, delete_stale=False)
        if res is not None:
            return res
        else:
            msg = "no offline db entry for file id %d" % (f_obj.file_id)
            raise TreeError(msg)

    def _gen_dir_entries_from_source(self, dir_obj, exclude_matcher=None):
        """Yield (basename, obj_id, is_file, rawmetadata)."""
        dir_id = dir_obj.dir_id
        for (obj_basename, obj_id, is_file) in self.db.get_dir_contents(dir_id):
            obj_type = FileItem if is_file else DirItem
            if obj_type == FileItem and exclude_matcher and \
                exclude_matcher.match_file_bname(obj_basename):
                pr.warning(
                    "excluded file %s at %s" % (
                        fstr2str(obj_basename),
                        self.printable_path(dir_obj.get_relpath())))
                yield (obj_basename, None, OtherItem, None)
            elif obj_type == DirItem and exclude_matcher and \
                exclude_matcher.match_dir_bname(obj_basename):
                pr.warning(
                    "excluded dir %s at %s" % (
                        fstr2str(obj_basename),
                        self.printable_path(dir_obj.get_relpath())))
                yield (obj_basename, None, OtherItem, None)
            else:
                yield (obj_basename, obj_id, obj_type, obj_id)

    def _new_file_obj(self, obj_id, rawmetadata):
        """Return new file object with given id, fill in metadata from
        raw_metadata. This method is set as _new_file_obj when mode is offline.
        """
        metadata = self.db.get_offline_metadata(obj_id)
        file_obj = self._file_type(obj_id, metadata)
        return file_obj

    def printable_path(self, rel_path, pprint=str):
        """Return a printable version of the tree+relpath."""
        return "{%s}%s" % \
                (pprint(fstr2str(self.db.dbpath)), pprint(fstr2str(rel_path)))

class FilePropTreeOnline(FilePropTree):
    """A FilePropertyTree that scans a mounted disk file tree and reads and
    updates a persistent cache database.
    """
    def get_prop(self, f_obj):
        """Return a file property value, from db if possible, updating db if
        needed. Raise TreeError if the value could not be obtained from
        database or from source.
        """
        # Try memory and database, without deleting entries.
        res = super(FilePropTreeOnline, self).get_prop(f_obj)
        if res is not None:
            return res
        if self.db_get_stored_prop(f_obj) is not None: # There is a stale entry.
            self.db.delete_ids(f_obj.file_id)
        try: # No value on memory or database cache, recompute.
            prop_value = self.prop_from_source(f_obj)
        except RuntimeError as exc:
            msg = "getting prop from source (id %d, %s): %s." % \
                  (f_obj.file_id, f_obj.relpaths, str(exc))
            pr.error(msg)
            self._rm_file(f_obj)
            raise_from(TreeError(msg), exc)
        f_obj.prop_value = prop_value
        f_obj.prop_metadata = f_obj.file_metadata
        try:
            self.db.set_prop_metadata(
                f_obj.file_id,
                prop_value,
                f_obj.prop_metadata)
        except RuntimeError as e:
            pr.error(
                "while saving file id %d prop to db: %s" \
                % (f_obj.file_id, str(e)))
        return prop_value

    def db_recompute_prop(self, file_obj):
        """Recompute property value for given file path.
        Raise RuntimeError if something goes wrong."""
        file_obj.prop_value = None            # Force prop recompute.
        try:
            self.db.delete_ids(file_obj.file_id)
        except PropDBError as exc:
            raise_from(RuntimeError(str(exc)), exc)
        self.get_prop(file_obj)

    def db_update_all(self):
        """Read property value for every file, forcing updates when needed."""
        error_files = set()
        update_files = set()
        try:
            for fobj, _parent, path \
                    in self.walk_paths(dirs=False, recurse=True):
                res = None
                try:
                    res = self.db_get_uptodate_prop(fobj, delete_stale=True)
                except TreeError as exc:
                    pr.error("processing file %s: %s" % (fstr2str(path), exc))
                    error_files.add(fobj)
                else:
                    if res is None:
                        update_files.add(fobj)
            for err_fobj in error_files:
                self._rm_file(err_fobj)
            tot_update_files = len(update_files)
            for index, update_fobj in enumerate(update_files):
                with pr.ProgressPrefix("%d/%d " % (index+1, tot_update_files)):
                    self.get_prop(update_fobj)
        finally:
            self.db.commit()

    def db_check_prop(self, file_obj):
        """Compare the file prop value from source against an up-to-date
        prop value in the db and return True or False accordingly.
        Do not update the database.
        Raise PropDBValueError if the db value is not up-to-date.
        """
        assert self.rootdir_obj is not None
        assert file_obj is not None and file_obj.is_file()
        db_prop = self.db_get_uptodate_prop(file_obj, delete_stale=False)
        if db_prop is None:
            raise PropDBValueError(
                "no uptodate prop/metadata for '%s'." % fstr2str(file_obj.relpaths[0]))
        live_prop_value = self.prop_from_source(file_obj)
        return db_prop == live_prop_value

    def db_purge_old_entries(self):
        """Scan tree and purge property database from old entries.
        Do not compact/vacuum the database."""
        self.scan_subtree()
        fileids_now = self._id_to_file.keys()
        pr.progress("discarding old ids from the db")
        self.db.delete_ids_except(fileids_now)
#        pr.progress("vacuuming database")
#        self.db.vacuum()
        pr.info("database cleaned")

    def printable_path(self, rel_path, pprint=str):
        """Return a printable version of the tree+relpath."""
        return super(FilePropTreeOnline, self).printable_path(
            rel_path, pprint=pprint)

    def db_store_offline(self, target_dbmanager):
        """Scan tree and save tree structure into the given target db manager.
        """
        assert self._use_metadata
        self.scan_subtree() # Scan the full tree.
        with target_dbmanager:
            self.db.merge_prop_values(target_dbmanager)
            target_dbmanager.rm_offline_tree()
            with pr.ProgressPrefix("saving hashes: "):
                tot_items = len(self._id_to_file)
                cur_item = 0
                for f_id, f_obj in iteritems(self._id_to_file):
                    pr.progress_percentage(cur_item, tot_items)
                    cur_item += 1
                    try:
                        target_dbmanager.set_offline_metadata(
                            f_id, f_obj.file_metadata)
                    except DBError as exc:
                        pr.error("saving offline metadata for id %d: %s" \
                            % (f_id, str(exc)))
            pr.progress("saving tree")
            for obj, parent, path in self.walk_paths(recurse=True, dirs=True):
                dir_id = parent.dir_id
                if obj.is_file():
                    obj_is_file = 1
                    obj_id = obj.file_id
                    obj_basename = os.path.basename(path)
                elif obj.is_dir():
                    obj_is_file = 0
                    obj_id = obj.dir_id
                    obj_basename = os.path.basename(path)
                target_dbmanager.set_dir_content(
                    dir_id, obj_basename, obj_id, obj_is_file)
