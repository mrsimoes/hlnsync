#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Provides a framework for persistent caching store of an integer-valued property
value associated to each file in a file tree.

On request, the property valued is read from persistent database cache, if it
has been stored and the file hasn't been modified since then (by metadata stamp,
i.e. size and mtime). Otherwise, the property valued is recomputed (method
prop_from_source) and stored in the persistent cache.

There are two modes of operation: ONLINE and OFFLINE.

- In ONLINE mode, access to the disk file tree is needed and the database may
be updated. In this mode, topdir_path must be a dir, the root of the disk file
tree.

- In OFFLINE mode, all property values as well as the tree data structure is
stored in the database. Access to the original source file tree is unneeded. In
this mode, topdir_path is set to None.

To use, implement online/offline alternative hierarchies for subclasses of
PropDBManager and FilePropTree using the protocol described in onlineoffline.py.

The database top class should be given by a kw argument "db", a callable that
takes a location, an optional mode (default ONLINE) and optional kwargs and
returns the appropriate db instance (e.g. the client subclass).

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

# pylint: disable=invalid-name, abstract-method, no-member
# pylint: disable=unused-import # Fails to detect metaclass parameters.

import os
import abc

import lnsync_pkg.printutils as pr
from lnsync_pkg.fileid import make_id_computer
from lnsync_pkg.miscutils import ListContextManager
from lnsync_pkg.modaltype import onofftype, Mode
from lnsync_pkg.filetree import \
    FileTree, FileItem, DirItem, ExcludedItem, TreeError
from lnsync_pkg.propdbmanager import PropDBException, PropDBError, \
    PropDBNoValue, PropDBStaleValue
from lnsync_pkg.sqlpropdb import SQLPropDBManager

class FileItemProp(FileItem):
    """
    A file tree object that records a file property value along with a
    metadata stamp, ie file size, mtime and ctime.
    """
    __slots__ = "prop_value", "prop_metadata"
    def __init__(self, obj_id, metadata):
        super(FileItemProp, self).__init__(obj_id, metadata)
        self.prop_value = None
        self.prop_metadata = None

class FilePropTree(FileTree, metaclass=onofftype):
    """
    A file tree providing persistent cache of values for a file property.
    A mode parameter at __new__ time determines which class is used for the
    instance.
    """

    def __init__(self, **kwargs):
        """
        kwargs specific to FilePropTree:
            dbmaker, a callable that accepts kwargs dbpath, and root.
            dbkwargs, args to be fed to the previous factory.
                If dbmaker is not none, it is called with root, mode and
                **dbkwargs to return a database manager object.
                (This delays connecting to the db as much as possible.)
        kwargs passed upwards to FileTree:
            - topdir_path: path disk file tree (may be None if FileTree is
                somehow virtual).
            - exclude_patterns: None or a list of glob patterns for
                relative paths to ignore when reading from disk.
            - use_metadata: if True read metadata index files by size
            - maxsize: ignore files larger than this, is positive and not None
            - skipempty: ignore zero-length files
            - writeback: if True, path operation methods update the disk tree.
            - file_type, dir_type: classes to instantiate.
        """
        topdir_path = kwargs.pop("topdir_path")
        self._enter_count = 0 # Context manager entry count.
        self.db = None
        use_metadata = kwargs.pop("use_metadata", True)
        assert use_metadata, \
            "__init__: must use_metadata"
        super(FilePropTree, self).__init__(
            topdir_path=topdir_path,
            use_metadata=use_metadata,
            file_type=FileItemProp,
            **kwargs)
        dbmaker = kwargs.get("dbmaker", SQLPropDBManager)
        if dbmaker:
            assert callable(dbmaker), \
                "__init__: expected a callable"
            dbobj = dbmaker(
                mode=self.mode, **kwargs.get("dbkwargs", {}))
            self.set_dbmanager(dbobj)

    def set_dbmanager(self, db):
        assert self.mode == db.mode, \
            "set_dbmanager: mismatched modes"
        self.db = db
        self.add_glob_patterns(db.get_glob_patterns())
        if self.mode == Mode.ONLINE:
            dbdir = os.path.dirname(db.dbpath)
            if  dbdir != self.topdir_path:
                self._id_computer = make_id_computer(dbdir)

    @abc.abstractmethod
    def prop_from_source(self, file_obj):
        """
        Obtain file property value from tree source.
        Raise RuntimeError if something goes wrong.
        """

    def open(self):
        self.db.open()

    def close(self):
        self.db.close()

    def __enter__(self):
        """
        Create the SQL db manager, scan root directory.
        """
        self._enter_count += 1
        if self._enter_count == 1: # First entered.
            self.db.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._enter_count -= 1
        res = False # Do not suppress any exception.
        if self._enter_count == 0:
            res = self.db.__exit__(exc_type, exc_val, exc_tb)
        return res

    @classmethod
    def listof(cls, init_args_list):
        """
        Return a context manager that will create/destroy a list of classref
        initialized by the given list of kwargs.
        """
        return ListContextManager(cls, init_args_list)

    def get_prop(self, f_obj):
        """
        Return the file property value, if at all possible.
        Otherwise, raise an appropriate PropDBException or TreeError.
        Do not delete stale DB values.
        """
        assert f_obj is not None, \
            "get_prop: no f_obj"
        if f_obj.prop_value is not None:
            return f_obj.prop_value
        f_obj.prop_value = self.db_get_uptodate_prop(f_obj, delete_stale=False)
        return f_obj.prop_value

    def db_get_uptodate_prop(self, f_obj, delete_stale):
        """
        Return either a property value, if on db and current.
        Raise PropDBNoValue or PropDBStaleValue if the value is either not in
        the db, or is stale: its metadata stamp (db mtime + size) does not match
        the file metadata.
        If the file property value is stale, optionally erase the stale entry
        from the db.
        May also raise PropDBError.
        """
        prop_val, prop_md = self.db.get_prop_metadata(f_obj.file_id)
        if prop_md == f_obj.file_metadata:
            f_obj.prop_value = prop_val
            f_obj.prop_metadata = prop_md
            return prop_val
        else:
            path_info = self.printable_file_path_digest(f_obj)
            pr.debug(
                "file id %d md changed %s->%s (del_stale:%s), paths:%s",
                f_obj.file_id, str(prop_md), str(f_obj.file_metadata),
                str(delete_stale), path_info)
            if delete_stale:
                self.db.delete_ids(f_obj.file_id)
            raise PropDBStaleValue

class FilePropTreeOffline(FilePropTree, mode=Mode.OFFLINE):
    """
    The file tree is stored in the property database along with up-to-date
    property values.
    """

    def __init___(self, **kwargs):
        topdir_path = kwargs.pop("topdir_path")
        assert topdir_path is None, \
            f"__init___: unexpected topdir_path: {topdir_path}"
        super().__init__(topdir_path=topdir_path, **kwargs)

    def db_get_uptodate_prop(self, f_obj, delete_stale):
        """
        Return the property value.
        Expect delete_stale should be False.
        Since the DB is offline, expect delete_stale to be False and raise
        stale and no-value conditions errors as PropDBError.
        """
        assert delete_stale is False, \
            "db_get_uptodate_prop: delete_stale set"
        try:
            return super(FilePropTreeOffline,
                         self).db_get_uptodate_prop(f_obj, delete_stale=False)
        except (PropDBNoValue, PropDBStaleValue) as exc:
            raise PropDBError("while offline: " + str(exc)) from exc

    def _gen_dir_entries_from_source(self, dir_obj, glob_matcher=None):
        """
        Yield (basename, obj_id, is_file, rawmetadata).
        """
        dir_id = dir_obj.dir_id
        for (obj_basename, obj_id, is_file) in self.db.get_dir_entries(dir_id):
            obj_type = FileItem if is_file else DirItem
            if obj_type == FileItem and glob_matcher and \
                glob_matcher.exclude_file_bname(obj_basename):
                pr.info(
                    "excluded file %s at %s" % (
                        obj_basename,
                        self.printable_path(dir_obj.get_relpath())))
                yield (obj_basename, None, ExcludedItem, None)
            elif obj_type == DirItem and glob_matcher and \
                glob_matcher.exclude_dir_bname(obj_basename):
                pr.info(
                    "excluded dir %s at %s" % (
                        obj_basename,
                        self.printable_path(dir_obj.get_relpath())))
                yield (obj_basename, None, ExcludedItem, None)
            else:
                # Files, not excluded.
                yield (obj_basename, obj_id, obj_type, obj_id)

    def _new_file_obj(self, obj_id, _rawmetadata):
        """
        Return new file object with given id, fill in metadata from
        raw_metadata. This method is set as _new_file_obj when mode is offline.
        """
        metadata = self.db.get_offline_metadata(obj_id)
        file_obj = self._file_type(obj_id, metadata)
        return file_obj

    def printable_path(self, rel_path=None, pprint=str):
        """
        Return a printable version of the tree+relpath.
        If rel_path is None, default to root directory.
        """
        if rel_path is None:
            rel_path = ""
        return "{%s}%s" % \
                (pprint(self.db.dbpath), pprint(rel_path))

class FilePropTreeOnline(FilePropTree, mode=Mode.ONLINE):
    """
    A FilePropertyTree that scans a mounted disk file tree and reads and
    updates a persistent cache database.
    """
    def __init__(self, **kwargs):
        topdir_path = kwargs.pop("topdir_path")
        assert os.path.isdir(topdir_path), \
            f"__init__: not a dir: {topdir_path}"
        super().__init__(topdir_path=topdir_path, **kwargs)

    def get_prop(self, f_obj):
        """
        Return a file property value, from db if possible, updating db if
        needed. Raise PropDBError if the DB cannot be read or TreeError if the
        value cannot be obtained from source.
        If the prop value cannot be written to the db, print an error message and continue.
        """
        # Try memory and database, without deleting entries.
        try:
            return super(FilePropTreeOnline, self).get_prop(f_obj)
        except PropDBStaleValue:
            self.db.delete_ids(f_obj.file_id)
        except PropDBNoValue:
            pass
        # No value on memory or database: recompute and store.
        try:
            prop_value = self.prop_from_source(f_obj)
        except TreeError:
            raise
#            self._rm_file(f_obj) # This impedes processing other files
                                  # in the same dir
                                  # TODO dynamic exclude
        except Exception:
            raise TreeError(
                    f"getting prop from source: {str(exc)}",
                    tree=self,
                    file_obj=f_obj)
        f_obj.prop_value = prop_value
        f_obj.prop_metadata = f_obj.file_metadata
        try:
            self.db.set_prop_metadata(f_obj.file_id,
                                      f_obj.prop_value,
                                      f_obj.prop_metadata)
        except PropDBError as exc:
            path_digest = self.printable_file_path_digest(f_obj)
            msg = f"while saving file id {f_obj.file_id} at {path_digest}: {str(exc)}"
            pr.error(msg)
        return f_obj.prop_value

    def recompute_prop(self, file_obj):
        """
        Recompute property value for given file path.
        May raise PropDBException.
        """
        file_obj.prop_value = None            # Force prop recompute.
        self.db.delete_ids(file_obj.file_id)
        self.get_prop(file_obj)

    def db_update_all(self):
        """
        Read property value for every file, forcing updates when needed.
        """
        error_files = set()
        update_files = set()
        try:
            for fobj, _parent, path \
                    in self.walk_paths(dirs=False, recurse=True):
                try:
                    self.db_get_uptodate_prop(fobj, delete_stale=True)
                except (PropDBNoValue, PropDBStaleValue):
                    update_files.add(fobj)
                except (PropDBError, TreeError) as exc:
                    pr.error("processing file: %s" % (exc,))
                    error_files.add(fobj)
            for err_fobj in error_files:
                self._rm_file(err_fobj)
            tot_update_files = len(update_files)
            for index, update_fobj in enumerate(update_files, start=1):
                with pr.ProgressPrefix("%d/%d " % (index, tot_update_files)):
                    try:
                        self.get_prop(update_fobj)
                    except (PropDBError, TreeError) as exc:
                        pr.error("computing hash: %s" % (exc,))
        finally:
            pr.progress("writing database...")
            self.db.commit()

    def db_check_prop(self, file_obj):
        """
        Compare the file prop value from source against an up-to-date
        prop value in the db and return True or False accordingly.
        Do not update the database.
        Raise PropDBStaleValue if the db value is not up-to-date.
        May raise TreeError if there was an error computing the prop from
        source.
        """
        assert self.rootdir_obj is not None, \
            "db_check_prop: missing root"
        assert file_obj is not None and file_obj.is_file(), \
            f"db_check_prop: not a file: {file_obj}"
        db_prop = self.db_get_uptodate_prop(file_obj, delete_stale=False)
        if db_prop is None:
            msg = "no up-to-date prop/metadata for " + \
                  self.printable_path(file_obj.relpaths[0])
            raise PropDBNoValue(msg)
        try:
            live_prop_value = self.prop_from_source(file_obj)
        except RuntimeError:
            raise PropDBError(
                "error hashing " + self.printable_path(file_obj.relpaths[0]))
        return db_prop == live_prop_value

    def db_purge_old_entries(self):
        """
        Scan tree and purge property database from old entries.
        Do not compact/vacuum the database.
        """
        self.scan_subtree()
        fileids_now = self._id_to_file.keys()
        pr.progress("discarding old ids from the db")
        self.db.delete_ids_except(fileids_now)
        pr.info("database cleaned")

    def printable_path(self, rel_path=None, pprint=str):
        """
        Return a printable version of the tree+relpath.

        If rel_path is None, default to root directory.
        """
        if rel_path is None:
            rel_path = ""
        return super(FilePropTreeOnline, self).printable_path(
            rel_path, pprint=pprint)

    def db_store_offline(self, target_dbmanager, filter_fn=None):
        """
        Scan tree and save tree structure into the given target db manager.

        filter_fn takes a file_id and returns True if the file is to be
        saved.
        """
        assert self._use_metadata, "db_store_offline: must be using metadata"
        self.scan_subtree() # Scan the full tree.

        def filter_include_only_files_in_subtree(file_id):
            # This excludes prop entries corresponding to files
            # that no longer exist or to files that are not in the subtree
            # that is benig saved, when using the db at a super-directory. 
            return file_id in self._id_to_file

        if filter_fn is None:
            actual_filter_fn = filter_include_only_files_in_subtree
        else:
            def actual_filter_fn(fid):
                return filter_include_only_files_in_subtree(fid) \
                    and filter_fn(fid)

        with target_dbmanager:
            pr.progress("merging databases...")
            self.db.merge_prop_values_into(
                target_dbmanager,
                filter_fn=actual_filter_fn)
            target_dbmanager.rm_offline_tree()

            pr.progress("saving metadata...")
            tot_items = len(self._id_to_file)
            cur_item = 0
            metadata_error_files = set()
            for f_id, f_obj in self._id_to_file.items():
                pr.progress_percentage(cur_item, tot_items)
                cur_item += 1
                if filter_fn and not filter_fn(f_obj.file_id):
                    continue
                try:
                    target_dbmanager.set_offline_metadata(
                        f_id, f_obj.file_metadata)
                except PropDBError as exc:
                    path_digest = self.printable_file_path_digest(f_obj)
                    pr.error(
                        f"saving offline metadata for id {f_id} " \
                        f"at {path_digest}: {str(exc)}")
                    metadata_error_files.add(f_obj)
                except Exception as exc:
                    v = exc
                    breakpoint()

            pr.progress("saving tree...")
            for obj, parent, path in self.walk_paths(recurse=True, dirs=True):
                dir_id = parent.dir_id
                if obj.is_file():
                    if obj in metadata_error_files or (filter_fn and not filter_fn(obj.file_id)):
                        continue
                    obj_is_file = 1
                    obj_id = obj.file_id
                    obj_basename = os.path.basename(path)
                elif obj.is_dir():
                    obj_is_file = 0
                    obj_id = obj.dir_id
                    obj_basename = os.path.basename(path)
                target_dbmanager.put_dir_entry(
                    dir_id, obj_basename, obj_id, obj_is_file)
