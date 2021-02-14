#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Implement an SQLite3 database suited  to FileProp, with ONLINE and OFFLINE
modes.

In offline mode, it stores a file tree structure as well as file metadata.

Open/close is achieved by managing a context. This is mandatory.

Instances are created with the actual SQLite3 database path (plus the mode
argument, to select online/offline).

In online mode, a root kw argument is accepted, so that the correct exclude
patterns are returned to make database files invisible to the tree. This
defaults to the directory containing the database file.

Connections to the same database file in online mode are shared via a class
variable.

All databases are EXCLUSIVE lock, i.e. lock on read.

Raise PropDBError if something goes wrong.

TODO DROP vs TRUNCATE
"""

# pylint: disable=unused-import # Fails to detect metaclass parameters.

import os
import sqlite3
import abc
import sys

from lnsync_pkg.fstr_type import fstr, fstr2str
from lnsync_pkg.miscutils import is_subdir
import lnsync_pkg.printutils as pr
from lnsync_pkg.glob_matcher import ExcludePattern
from lnsync_pkg.filetree import Metadata
from lnsync_pkg.modaltype import ONLINE, OFFLINE
from lnsync_pkg.proptree import PropDBManager, PropDBError, PropDBNoValue

CUR_DB_FORMAT_VERSION = 1

# From fstr to db value and back.
_SQL_TEXT_FACTORY = lambda x: x
_SQL_TEXT_STORER = lambda x: x

def mk_online_db(dir_path, db_basename):
    return SQLPropDBManager(os.path.join(dir_path, db_basename), mode=ONLINE)

class SQLPropDBManager(PropDBManager):
    """
    Manage an SQLite3 file property db for FilePropertyTree.

    Implement a context manager protocol that either creates a sqlite
    database connection or reuses a previously existing connection
    if a database is shared (--root option).

    In online mode, the tree root is required to figure out if
    database temp files need to be excluded.
    """
    _current_online_cx = {} # dbpath -> [enter_count, db_cx].

    def __init__(self, dbpath, **kwargs):
        """
        dbpath is the actual sqlite3 database filename.
        """
        self._cx = None
        self._enter_count = 0
        self.dbpath = dbpath
        super(SQLPropDBManager, self).__init__(dbpath, **kwargs)

    # (table_name, fields_including_key, optional_index)
    _tables_prop = \
    [("prop",
      "file_id INT PRIMARY KEY, value INT, size INT, mtime INT, ctime INT",
      "file_id"
     )]

    # (table_name, fields_including_key, optional_index)
    _tables_offline = \
        [("dir_contents",
          "parent_id INT, obj_basename TEXT, obj_id INT, obj_is_file INT, "
          "PRIMARY KEY (parent_id, obj_basename)",
          "obj_id, obj_is_file"),
         ("metadata",
          "file_id INT, size INT, mtime INT, ctime INT, "
          "FOREIGN KEY (file_id) REFERENCES prop(file_id) "
          "PRIMARY KEY (file_id)",
          None)]

    def __enter__(self):
        sql_db_path = self.dbpath
        if sql_db_path in self._current_online_cx:
            cx_info = self._current_online_cx[sql_db_path]
            cx_info[0] += 1
            self._cx = cx_info[1]
        else:
            if not os.path.isfile(sql_db_path):
                self._create_empty()
            ver = self.which_db_version()
            if ver is None:
                raise PropDBError("unreadable DB at %s" % fstr2str(sql_db_path))
            elif ver < CUR_DB_FORMAT_VERSION:
                msg = "outdated db version=%d at %s" \
                      % (ver, fstr2str(sql_db_path))
                raise PropDBError(msg)
            try:
                self._cx = sqlite3.connect(fstr2str(sql_db_path))
#                def factory(string):
#                    print("factory: ", string)
#                    import pdb; pdb.set_trace()
#                    return sql_text_factory(string)
#                self._cx.text_factory = factory
# #text_factory not used under Python 3, apparently
            except sqlite3.Error as exc:
                msg = "cannot open DB at %s" % fstr2str(sql_db_path)
                raise PropDBError(msg) from exc
            self._current_online_cx[sql_db_path] = [1, self._cx]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._cx:
            sql_db_path = self.dbpath
            if sql_db_path in self._current_online_cx:
                cx_info = self._current_online_cx[sql_db_path]
                cx_info[0] -= 1
                if cx_info[0] == 0:
                    del self._current_online_cx[sql_db_path]
                    self._cx.commit()
                    self._cx.close()
        return False # Do not suppress any exception.

    def set_prop_metadata(self, file_id, prop_value, metadata):
        cmd = "INSERT INTO prop VALUES (?, ?, ?, ?, ?);"
        cmd_args = (file_id, prop_value,
                    metadata.size, metadata.mtime, metadata.ctime)
        try:
            self._cx.execute(cmd, cmd_args)
        except sqlite3.Error as exc:
            msg = "setting metadata: %s" % (fstr2str(self.dbpath),)
            raise PropDBError(msg) from exc

    def get_prop_metadata(self, file_id):
        """
        Get the property value and the metadata associated with the file at
        the time the property was computed.
        Return (prop_value, metadata) or raise PropDBError.
        """
        try:
            cmd = "SELECT value, size, mtime, ctime FROM prop WHERE file_id=?;"
            res = self._cx.execute(cmd, (file_id,)).fetchone()
        except sqlite3.Error as exc:
            raise PropDBError("reading: %s" % (fstr2str(self.dbpath),)) from exc
        if res is None:
            raise PropDBNoValue("no value for file id %d at DB %s" % \
                    (file_id, fstr2str(self.dbpath)))
        elif not isinstance(res[0], int):
            raise PropDBError("prop value not integer for file id %d" % (file_id,))
        res = (res[0], Metadata(res[1], res[2], res[3]))
        return res

    def _create_empty(self):
        """
        Create a new SQL database file and close it.
        """
        dbpath = self.dbpath
        assert not os.path.exists(dbpath)
        pr.progress("Creating new database %s ..." % (fstr2str(dbpath)))
        temp_cx = None
        try:
            temp_cx = sqlite3.connect(fstr2str(dbpath))
            SQLPropDBManager._reset_db_tables(temp_cx, self._all_tables())
            temp_cx.execute(
                "PRAGMA user_version=%d;" % int(CUR_DB_FORMAT_VERSION))
            temp_cx.execute("PRAGMA locking_mode=EXCLUSIVE;")
            temp_cx.execute("PRAGMA foreign_keys=ON;")
        except sqlite3.Error as exc:
            msg = "cannot create database at %s", fstr2str(dbpath)
            raise PropDBError(msg) from exc
        finally:
            if temp_cx is not None:
                temp_cx.commit()
                temp_cx.close()

    @abc.abstractmethod
    def _all_tables(self):
        pass

    def which_db_version(self):
        """
        Return db version number or None if not recognized.
        """
        db_path = self.dbpath
        sql_cx = None
        db_ver = None
        try:
            sql_cx = sqlite3.connect(fstr2str(db_path))
            db_ver_rec = sql_cx.execute("PRAGMA user_version;").fetchone()
            if db_ver_rec is not None:
                db_ver = db_ver_rec[0]
            else:
                db_ver = 0
        except sqlite3.Error as exc:
            msg = "cannot open DB at %s" % fstr2str(db_path)
            raise PropDBError(msg) from exc
        finally:
            if sql_cx:
                sql_cx.close()
        return db_ver

    def rm_offline_tree(self):
        """
        Remove offline tree (even in online mode).
        """
        SQLPropDBManager._reset_db_tables(self._cx, self._tables_offline)
        self._cx.commit()

    @staticmethod
    def _reset_db_tables(sql_cx, tables):
        for tab in tables:
            SQLPropDBManager._reset_db_table(sql_cx, tab)

    @staticmethod
    def _reset_db_table(sql_cx, table):
        tab_name, fields, index = table
        sql_cx.execute("DROP TABLE IF EXISTS %s;" % tab_name)
        sql_cx.execute("CREATE TABLE %s (%s);" % (tab_name, fields))
        if index is not None:
            cmd = \
                "CREATE INDEX %sidx ON %s (%s);" % (tab_name, tab_name, index)
            sql_cx.execute(cmd)
        sql_cx.commit()

    def commit(self):
        self._cx.commit()

    def compact(self):
        self._cx.commit()
        self._cx.execute("VACUUM;")

    def merge_prop_values(self, tgt_db, remap_id_fn=None, filter_fn=None):
        """
        Update db at target with prop values from source, overwriting if
        necessary.
        """
        tgt_cx = tgt_db._cx
        prop_tab = self._tables_prop[0]
        tab_name = prop_tab[0]
        if remap_id_fn is None and filter_fn is None:
            tgt_cx.execute("ATTACH ? AS SOURCE;", (self.dbpath,))
            cmd = ("DELETE FROM %s "
                   "WHERE file_id IN (SELECT file_id FROM SOURCE.%s) ;") \
                    % (tab_name, tab_name)
            tgt_cx.execute(cmd)
            cmd = "INSERT INTO %s SELECT * FROM SOURCE.%s ;" \
                  % (tab_name, tab_name)
            tgt_cx.execute(cmd)
        else:
            get_cmd = "SELECT * FROM %s;" % (tab_name,)
            get_cursor = self._cx.cursor()
            test_cursor = tgt_cx.cursor()
            put_cursor = tgt_cx.cursor()
            cmd_test_if = "SELECT * FROM %s WHERE file_id=?;" % tab_name
            cmd_delete = "DELETE FROM %s WHERE file_id=?;" % tab_name
            cmd_insert = "INSERT INTO %s VALUES (?, ?, ?, ?, ?);" % tab_name
            for res in get_cursor.execute(get_cmd).fetchall():
                # Apply map to fileid.
                if remap_id_fn is None:
                    resout = res
                else:
                    resout = (remap_id_fn(res[0]), \
                              res[1], res[2], res[3], res[4])
                if filter_fn is None or filter_fn(resout[0]):
                    test_cursor.execute(cmd_test_if, (resout[0],))
                    if test_cursor.fetchall():
                        test_cursor.execute(cmd_delete, (resout[0],))
                    put_cursor.execute(cmd_insert, resout)
        tgt_db.compact()

class SQLPropDBManagerOffline(SQLPropDBManager, mode=OFFLINE):
    """
    Manage an SQLite3 file property db for FilePropertyTree.

    In offline mode, the database contains the file tree structure
    and file metadata (size, mtime, ctime).
    """
    def __enter__(self):
            # Make sure we have the root directory contents, at least.
        super(SQLPropDBManagerOffline, self).__enter__()
        try:
            _res = list(self.get_dir_entries(0)) # Force evaluation.
        except Exception as exc:
            self.__exit__(*sys.exc_info())
            msg = "not an offline database, was it created with mkoffline? %s"
            raise PropDBError(msg % (fstr2str(self.dbpath),)) from exc
        return self

    def _all_tables(self):
        return SQLPropDBManager._tables_prop \
             + SQLPropDBManagerOffline._tables_offline

    def set_offline_metadata(self, f_id, metadata):
        """
        Store file metadata to the database.
        """
        cmd = "INSERT INTO metadata VALUES (?, ?, ?, ?);"
        self._cx.execute(
            cmd, (f_id, metadata.size, metadata.mtime, metadata.ctime))

    def get_offline_metadata(self, file_id):
        """
        Return file offline saved metadata from database, or raise PropDBError.
        """
        cmd = "SELECT size, mtime, ctime FROM metadata WHERE file_id=?;"
        try:
            size, mtime, ctime = self._cx.execute(cmd, (file_id,)).fetchone()
        except Exception as exc:
            msg = "Cannot read database %s offline metadata for file id %d." \
                % (fstr2str(self.dbpath), file_id,)
            raise PropDBError(msg) from exc
        return Metadata(size, mtime, ctime)

    def put_dir_entry(self, dir_id, obj_basename, obj_id, obj_is_file):
        """
        Store a dir entry into the database-stores file tree.
        """
        # Escape single quotes for sqlite3.
        def storer(string):
            string.replace(fstr("'"), fstr("''"))
            return _SQL_TEXT_STORER(string)
        cmd = "INSERT INTO dir_contents VALUES (?, ?, ?, ?);"
        self._cx.execute(
            cmd, (dir_id, storer(obj_basename), obj_id, obj_is_file))

    def get_dir_entries(self, dir_id):
        """
        Generate dir entries from the database-stored file tree.
        """
        cmd = "SELECT obj_basename, obj_id, obj_is_file " + \
              "FROM dir_contents WHERE parent_id=?;"
        cur = self._cx.execute(cmd, (dir_id,))
        for db_rec in cur.fetchall():
            yield (_SQL_TEXT_FACTORY(db_rec[0]), db_rec[1], db_rec[2])

class SQLPropDBManagerOnline(SQLPropDBManager, mode=ONLINE):
    """
    In online mode:
    - Files related to SQL database are ignored--exclude.
      (For this, the tree root dir is needed.))
    - File ids may be removed.
    - File property values may be merged in.
    """
    def __init__(self, dbpath, root_path=None, **kwargs):
        self.treeroot = root_path
        super(SQLPropDBManagerOnline, self).__init__(
            dbpath, root=root_path, **kwargs)

    def get_glob_patterns(self):
        """
        Exclude the SQLite3 main database and -journal, -wal and other tmp
        files. The database may be located anywhere, even away from the tree
        root.
        """
        if self.treeroot and is_subdir(self.dbpath, self.treeroot):
            relpath = os.path.relpath(self.dbpath, self.treeroot)
            return [ExcludePattern(fstr("/") + relpath),
                    ExcludePattern(fstr("/") + relpath + fstr("-*"))]
        else:
            return []

    def _all_tables(self):
        return SQLPropDBManager._tables_prop

    def delete_ids(self, file_ids):
        """
        Remove single file_id, or list or set of file_ids from property table.
        """
        if isinstance(file_ids, int):
            file_ids = (file_ids,)
        try:
            del_prop_cmd = "DELETE FROM prop WHERE file_id=?;"
            vals = [(fid,) for fid in file_ids]
            self._cx.executemany(del_prop_cmd, vals)
        except sqlite3.Error as exc:
            msg = "could not delete from %s file ids: %s (%s)" % \
                (fstr2str(self.dbpath), file_ids, exc)
            raise PropDBError(msg) from exc

    def delete_ids_except(self, file_ids_to_keep):
        """
        Delete from the db all ids, exdeletecept those given. Expensive.
        """
        ids_to_delete = set()
        if not isinstance(file_ids_to_keep, set):
            file_ids_to_keep = set(file_ids_to_keep)
        pr.progress("reading from database")
        tot_file_records = self._cx.execute(
            "SELECT count(*) FROM prop;").fetchone()[0]
        curr_record = 0
        with pr.ProgressPrefix("pruning: "):
            for fileid_record in self._cx.execute(
                    "SELECT file_id FROM prop;").fetchall():
                pr.progress_percentage(curr_record, tot_file_records)
                curr_record += 1
                this_id = fileid_record[0]
                if not this_id in file_ids_to_keep:
                    ids_to_delete.add(this_id)
        self.delete_ids(ids_to_delete)
