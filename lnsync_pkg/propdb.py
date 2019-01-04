#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""Provides a framework for persistent caching store of an integer-valued
property value associated to each file in a file tree.

On request the property valued is read from cache, if it has been stored
there and the file hasn't been modified since then. Otherwise, the property
valued is recomputed and stored in the persistent cache.

There are two modes of operation:

- In "online" mode, access to the source file tree is needed and the database
may be updated. To check for stale property values, files metadata such as
modification time and size is accessed. The cache is located at the file tree
root directory. The database file itself is invisible in the API.

- In "offline" mode, access to the source file tree is never needed. All
pertinent file information is cached and a current property value for each
file has been obtained and stored. The database itself is located anywhere
and no reference to its original root directory path is needed, since all
paths in the API are relative.

To use the framework, implement:
    prop_from_source - compute the property value from the source tree (int or long int).

The cache is an SQL database. To properly close the database, use this class
as a context manager:
    with database_obj as var:
        <body>
To set up a context for using multiple FilePropertyDB, use:
    with FilePropertyDBs(propdb_list) as var:
        <body>
with var be bound to a list of FilePropertyDB.
"""

from __future__ import with_statement, print_function

import os
import sys
import abc
import pipes
from collections import namedtuple
import sqlite3

import lnsync_pkg.printutils as pr
from lnsync_pkg.filetree import FileTree, FileObj, DirObj, Metadata

CUR_DB_FORMAT_VERSION = 1

class FileObjProp(FileObj):
    __slots__ = "prop_value", "prop_metadata"
    def __init__(self, obj_id, metadata):
        super(FileObjProp, self).__init__(obj_id, metadata)
        self.prop_value = None
        self.prop_metadata = None # File metadata when property was last computed.

class FilePropertyDB(FileTree):
    def __init__(self, dbpath=None, mode=None, maxsize=None):
        """Opens or creates a new database.

        dbpath - path to a database file, existing or to be created.
        If maxsize is specified, files larger than maxsize are ignored.
        """
        assert mode in ("online", "offline")
        assert not os.path.exists(dbpath) or os.path.isfile(dbpath)
        dbpath = os.path.realpath(os.path.expanduser(dbpath))
        self._enter_count = 0 # Reentrant context manager.
        self.mode = mode
        self.dbpath = dbpath
        self.db_basename = None
        self.sqlmanager = None
        super(FilePropertyDB, self).__init__(root_path=os.path.dirname(dbpath), \
            maxsize=maxsize, use_metadata=True)
        if self.mode == "offline":
            self.new_file_obj = self._new_file_obj_offline
            self._gen_source_dir_entries = self._gen_source_dir_entries_offline
        self._file_type = FileObjProp

    def __enter__(self):
        # The root directory object is created only when entering a managed context.
        self._enter_count += 1
        if self._enter_count == 1: # First entered.
            if self.mode == "online": # Read tree, then create/open db, avoid db temp files.
                # Scan root dir before db tmp files are created.
                super(FilePropertyDB, self).__enter__()
                self.db_basename = os.path.basename(self.dbpath)
                self.scan_dir(self.rootdir_obj, skipbasenames=[self.db_basename])
                self._create_sql_manager(self.dbpath)
            else:
                self._create_sql_manager(self.dbpath)
                super(FilePropertyDB, self).__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._enter_count -= 1
        if self._enter_count == 0:
            res = super(FilePropertyDB, self).__exit__(exc_type, exc_val, exc_tb)
            del self.sqlmanager
            return res
        else:
            return False # Do not suppress any exception.

    def _create_sql_manager(self, dbpath):
        """To be overridden if no SQL database is needed.
        """
        if not os.path.exists(dbpath):
            self.sqlmanager = SQLPropDBManager.create_new_db(dbpath)
        else:
            self.sqlmanager = SQLPropDBManager(dbpath)

    @abc.abstractmethod
    def prop_from_source(self, file_obj):
        """Should obtain file property value from tree source."""

    def _gen_source_dir_entries_offline(self, dir_obj, skipbasenames=None):
        """Yield (basename, obj_id, is_file, rawmetadata).
        """
        dir_id = dir_obj.dir_id
        for (obj_basename, obj_id, is_file) in self.sqlmanager.get_dir_contents(dir_id):
            if skipbasenames is not None and obj_basename in skipbasenames:
                continue
            obj_type = FileObj if is_file else DirObj
            yield (obj_basename, obj_id, obj_type, obj_id)

    def _new_file_obj_offline(self, obj_id, raw_metadata):
        """Return file metadata created from raw_metadata input.
        """
        metadata = self.sqlmanager.get_metadata(obj_id)
        file_obj = self._file_type(obj_id, metadata)
        return file_obj

    def scan_dir(self, dir_obj, skipbasenames=None):
        """Skip own database file when scanning root in online mode.
        """
        if dir_obj is self.rootdir_obj and self.mode == "online" and skipbasenames is None:
            skipbasenames = [self.db_basename]
        super(FilePropertyDB, self).scan_dir(dir_obj, skipbasenames=skipbasenames)

    def db_purge(self):
        """Purge DB from old entries and perform SQL vacuum.
        """
        assert self.mode == "online"
        self.scan_full_tree()
        fileids_now = self._id_to_file.keys()
        pr.progress("discarding old ids from the db")
        self.sqlmanager.delete_ids_except(fileids_now)
        pr.progress("vacuuming database")
        self.sqlmanager.vacuum()
        pr.info("database cleaned")

    def db_get_prop(self, f_obj):
        """Return a file property value, from db if possible, updating db if needed.

        Raise RuntimeError if the value could not be obtained.
        """
        assert f_obj is not None, "db_get_prop: no f_obj"
        if f_obj.prop_value is not None:
            return f_obj.prop_value
        else:
            res = None
            try:
                res = self.sqlmanager.get_prop_metadata(f_obj.file_id)
            except Exception:
                msg = "error reading prop db for file id '%d'." % (f_obj.file_id)
                raise RuntimeError(msg)
            if res is not None:
                f_obj.prop_value = res[0]
                f_obj.prop_metadata = res[1]
                if f_obj.prop_metadata == f_obj.file_metadata:
                    return f_obj.prop_value
                else:
                    pr.debug(
                        "file %s md (id %d) changed: was %s, is %s",
                        f_obj.relpaths[0], f_obj.file_id,
                        f_obj.prop_metadata, f_obj.file_metadata)
                    self.sqlmanager.delete_ids(f_obj.file_id)
        # Property needs to be computed and, in online mode, stored in the db.
        if self.mode == "offline":
            raise RuntimeError("no prop on db for file id '%d'." % (f_obj.file_id))
        try:
            prop_value = self.prop_from_source(f_obj)
        except Exception:
            raise RuntimeError("error processing file %d, %s." % (f_obj.file_id, f_obj.relpaths))
        f_obj.prop_value = prop_value
        f_obj.prop_metadata = f_obj.file_metadata
        try:
            self.sqlmanager.set_prop_metadata(
                f_obj.file_id,
                prop_value,
                f_obj.prop_metadata)
        except Exception:
            pr.warning("could not save prop value for file id '%d'." % f_obj.file_id)
        return prop_value

    def db_check_prop(self, file_obj):
        """Recompute prop value for file fname_rel.

        Is it the same as in the db? return [].
        Different? Return [new_prop].
        Do not update the DB.
        """
        assert self.mode == "online" and self.rootdir_obj is not None
        assert file_obj is not None and file_obj.is_file()
        cached_prop_times = self.sqlmanager.get_prop_metadata(file_obj.file_id)
        if cached_prop_times is None:
            raise RuntimeError("no prop value cached for %s." % file_obj.relpaths)
        else:
            cached_prop = cached_prop_times[0]
        live_prop_value = self.prop_from_source(file_obj)
        return cached_prop == live_prop_value

    def db_store_tree(self):
        """Save tree info to db, for offline usage.
        """
        assert self.mode == "online", "db is not online"
        assert self._use_metadata, "not using metadata"
        self.scan_full_tree() # Setup _id_to_file.
        self.sqlmanager.reset_offline_tables()
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
            self.sqlmanager.set_dir_content(dir_id, obj_basename, obj_id, obj_is_file)
        tot_items = len(self._id_to_file)
        cur_item = 0
        for f_id, f_obj in self._id_to_file.iteritems():
            pr.progress_percentage(cur_item, tot_items, prefix="updating file metadata")
            cur_item += 1
            self.sqlmanager.set_metadata(f_id, f_obj.file_metadata)
        self.sqlmanager.commit()

    def db_clear_tree(self):
        """Remove offline tree info from db.
        """
        self.sqlmanager.reset_offline_tables()
        self.sqlmanager.vacuum()

    def db_update_all(self):
        """Read prop value for every file, forcing updates when needed.

        Also, delete from the db all files which could not be read.
        """
        assert self.mode == "online"
        error_files = set()
        update_files = set()
        try:
            for fobj, parent, path in self.walk_paths(dirs=False, recurse=True):
                res = None
                try:
                    res = self.sqlmanager.get_prop_metadata(fobj.file_id)
                except Exception as exc:
                    pr.warning("error processing file %s" % path)
                    pr.debug(exc)
                    error_files.add(fobj)
                else:
                    if res is None:
                        update_files.add(fobj)
            for err_fobj in error_files:
                self._rm_file(err_fobj)
            tot_update_files = len(update_files)
            try:
                old_prefix = pr.PROGRESS_PREFIX
                for index, update_fobj in enumerate(update_files):
                    pr.PROGRESS_PREFIX = "%d/%d " % (index+1, tot_update_files)
                    self.db_get_prop(update_fobj)
            finally:
                pr.PROGRESS_PREFIX = old_prefix
        finally:
            self.sqlmanager.commit()

    def do_recompute_file(self, file_obj):
        """Recompute property value for given file path.
        """
        file_obj.prop_value = None            # Force prop recompute.
        self.sqlmanager.delete_ids(file_obj.file_id)
        self.db_get_prop(file_obj)

    def printable_path(self, rel_path):
        if self.mode == "offline":
            return "{%s}%s" % (pipes.quote(self.dbpath), pipes.quote(rel_path))
        else:
            return pipes.quote(self.rel_to_abs(rel_path))

    def print_tree(self):
        for fobj, parent, path in self.walk_paths(dirs=False, recurse=True):
            print(path, " size: %d, hash: %d" % \
                    (fobj.file_metadata.size, self.db_get_prop(fobj)))


PropRecord = namedtuple("PropRecord", ["prop", "file_id", "value", "size", "mtime", "ctime"])

MetadataRecord = namedtuple("MetadataRecord", ["file_id", "size", "mtime", "ctime"])

class SQLPropDBManager(object):
    """Interfaces FilePropertyDB to the SQL database. All SQL commands go here.
    """
    # table data: (table_name, fields_including_key, optional_index)
    _prop_table_info = \
    ("prop", "file_id INT, value INT, size INT, mtime INT, ctime INT", "file_id")
    _offline_tables_info = \
        (("dir_contents",
          "parent_id INT, obj_basename TEXT, obj_id INT, obj_is_file INT, " \
          + " PRIMARY KEY (parent_id, obj_basename)",
          "obj_id, obj_is_file"),
         ("metadata", "file_id INT, size INT, mtime INT, ctime INT, " \
          + "PRIMARY KEY (file_id)", None))

    @staticmethod
    def create_new_db(dbpath):
        """Create a new db with requisite tables and return new SQLPropDBManager.
        """
        if os.path.exists(dbpath):
            raise RuntimeError("file already exists at %s" % dbpath)
        pr.info("will create '%s'." % dbpath)
        sql_cx = None
        try:
            sql_cx = sqlite3.connect(dbpath)
            SQLPropDBManager._reset_prop_table(sql_cx)
            SQLPropDBManager._reset_offline_tables(sql_cx)
            sql_cx.execute("PRAGMA user_version=%d;" % int(CUR_DB_FORMAT_VERSION))
        except sqlite3.Error:
            pr.error("cannot create database at %s.", dbpath)
            raise
        finally:
            if sql_cx is not None:
                sql_cx.close()
        return SQLPropDBManager(dbpath)

    @staticmethod
    def _reset_prop_table(sql_cx):
        SQLPropDBManager._reset_db_table(sql_cx, SQLPropDBManager._prop_table_info)

    @staticmethod
    def _reset_offline_tables(sql_cx):
        for tab in SQLPropDBManager._offline_tables_info:
            SQLPropDBManager._reset_db_table(sql_cx, tab)

    @staticmethod
    def _reset_db_table(sql_cx, table):
        tab_name, fields, index = table
        sql_cx.execute("DROP TABLE IF EXISTS %s;" % tab_name)
        sql_cx.execute("CREATE TABLE %s (%s);" % (tab_name, fields))
        if index is not None:
            cmd = "CREATE INDEX %sidx ON %s (%s);" % (tab_name, tab_name, index)
            sql_cx.execute(cmd)
        sql_cx.commit()

    @staticmethod
    def which_db_version(dbpath):
        """Return db version number or None if not recognized.
        """
        sql_cx = None
        db_ver = None
        try:
            sql_cx = sqlite3.connect(dbpath)
            db_ver_rec = sql_cx.execute("PRAGMA user_version;").fetchone()
            if db_ver_rec is not None:
                db_ver = db_ver_rec[0]
            else:
                db_ver = 0
        finally:
            if sql_cx:
                sql_cx.close()
        return db_ver

    @staticmethod
    def copy_db(src_db_path, tgt_db_path, remap_fn=lambda a: a, copyif_fn=lambda a: True):
        """Copy all entries of source db to target db, applying remap_fn to each file_id.
            remap_fn(fid) -> new_fid and copyf_fn(fid) -> Boolean.

        Overwrites the db located at tgt_db_path, if any.
        """
        src_sqlm = SQLPropDBManager(src_db_path)
        if os.path.isfile(tgt_db_path):
            os.remove(tgt_db_path)
        tgt_sqlm = SQLPropDBManager.create_new_db(tgt_db_path)

        src_cx = src_sqlm._cx
        tgt_cx = tgt_sqlm._cx

        tgt_cx.cursor().execute("DELETE FROM prop;")

        tgt_cur = tgt_cx.cursor()
        selcmd = "SELECT file_id, value, size, mtime, ctime FROM prop WHERE file_id>0;"
        for fid, propv, size, mtime, ctime in src_cx.execute(selcmd).fetchall():
            if copyif_fn(fid):
                fid = remap_fn(fid)
                pr.progress("inserting file id %d" % fid)
                cmd = "INSERT INTO prop VALUES (?, ?, ?, ?, ?);"
                tgt_cur.execute(cmd, (fid, propv, size, mtime, ctime))
        tgt_sqlm.vacuum() # This also commits.

    def __init__(self, dbpath):
        """The db must exist and be in a current format, else raise exceptions.
        """
        self._cx = None
        self.dbpath = dbpath
        if not os.path.isfile(dbpath):
            raise RuntimeError("unreadable DB at %s" % dbpath)
        ver = SQLPropDBManager.which_db_version(dbpath)
        if ver is None:
            raise RuntimeError("unreadable DB at %s" % dbpath)
        elif ver < CUR_DB_FORMAT_VERSION:
            msg = "outdated db version=%d at %s" % (ver, self.dbpath)
            raise RuntimeError(msg)
        try:
            self._cx = sqlite3.connect(dbpath)
            def fac(text):
                return text
            self._cx.text_factory = fac
        except sqlite3.Error:
            pr.error("cannot open DB at '%s'.", dbpath)
            raise

    def __del__(self):
        if self._cx:
            self._cx.commit()
            self._cx.close()

    def reset_offline_tables(self):
        SQLPropDBManager._reset_offline_tables(self._cx)

    def delete_ids(self, file_ids):
        """Remove single file_id, or list or set of file_ids, from db.
        """
        if isinstance(file_ids, (long, int)):
            file_ids = (file_ids,)
        try:
            del_cmd = "DELETE FROM prop WHERE file_id=?;"
            self._cx.executemany(del_cmd, [(fid,) for fid in file_ids])
        except Exception:
            pr.error("could not delete ids.")
            raise

    def delete_ids_except(self, file_ids_to_keep):
        """Delete from the db all ids, except those given. Costly.
        """
        ids_to_delete = set()
        pr.progress("reading all file ids from the database")
        tot_file_records = self._cx.execute("SELECT count(*) FROM prop;").fetchone()[0]
        curr_record = 0
        for fileid_record in self._cx.execute("SELECT file_id FROM prop;").fetchall():
            pr.progress_percentage(curr_record, tot_file_records, "deleting other ids: ")
            curr_record += 1
            this_id = fileid_record[0]
            if not this_id in file_ids_to_keep:
                ids_to_delete.add(this_id)
        self.delete_ids(ids_to_delete)

    def get_metadata(self, file_id):
        """Fetch file metadata from the database.
        """
        cmd = "SELECT size, mtime, ctime FROM metadata WHERE file_id=?;"
        size, mtime, ctime = self._cx.execute(cmd, (file_id,)).fetchone()
        return Metadata(size, mtime, ctime)

    def set_metadata(self, f_id, metadata):
        """Store file metadata in the database.
        """
        cmd = "INSERT INTO metadata VALUES (?, ?, ?, ?);"
        self._cx.cursor().execute(cmd, (f_id, metadata.size, metadata.mtime, metadata.ctime))

    def get_dir_contents(self, dir_id):
        """Generate dir entries from the database to build the tree.
        """
        cmd = "SELECT obj_basename, obj_id, obj_is_file FROM dir_contents WHERE parent_id=?;"
        try:
            cur = self._cx.execute(cmd, (dir_id,))
            for db_rec in cur.fetchall():
                yield db_rec
        except:
            pr.error("error getting contents.")
            raise

    def set_dir_content(self, dir_id, obj_basename, obj_id, obj_is_file):
        """Store a dir entry into the database.
        """
        obj_basename = obj_basename.replace("'", "''") # Escape single quotes for sqlite3.
        cmd = "INSERT INTO dir_contents VALUES (?, ?, ?, ?);"
        self._cx.cursor().execute(cmd, (dir_id, obj_basename, obj_id, obj_is_file))

    def get_prop_metadata(self, file_id):
        """Return either a (prop_value, metadata) pair or None.

        May raise an SQL error or RuntimeError,
        """
        cmd = "SELECT value, size, mtime, ctime FROM prop WHERE file_id=?;"
        res = self._cx.execute(cmd, (file_id,)).fetchone()
        if res is not None:
            if not isinstance(res[0], (int, long)):
                raise RuntimeError("prop value not integer.")
            res = (res[0], Metadata(res[1], res[2], res[3]))
        return res

    def set_prop_metadata(self, file_id, prop_value, metadata):
        cmd = "INSERT INTO prop VALUES (?, ?, ?, ?, ?);"
        cmd_args = (file_id, prop_value,
                    metadata.size, metadata.mtime, metadata.ctime)
        self._cx.cursor().execute(cmd, cmd_args)

    def vacuum(self):
        self._cx.execute("VACUUM;")
        self.commit()

    def commit(self):
        self._cx.commit()


def copy_db(src_db_path, tgt_db_path):
    """Copy SQL database filtering or mapping of the data.
    """
    SQLPropDBManager.copy_db(src_db_path, tgt_db_path)


class FilePropertyDBs(object):
    """Manage a context for multiple PropDB objects.
    """
    def __init__(self, db_l):
        self.dbs = []
        try:
            for database in db_l:
                self.dbs.append(database)
                self.dbs[-1].__enter__()
        except Exception as exc:
            traceback = sys.exc_info()[2]
            if not self.__exit__(type(exc), exc, traceback):
                raise type(exc), exc, traceback
    def __enter__(self):
        return self.dbs
    def __exit__(self, exc_type, exc_value, traceback):
        res = False # By default, the exception was not handled here.
        for database in self.dbs:
            res = database.__exit__(exc_type, exc_value, traceback) or res
        return res
