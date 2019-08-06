#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Implement an SQLite3 database suited  to FileProp, with "online" and "offline"
modes.

In offline mode, stores a file tree.

Open/close is achieved by managing a context. This is mandatory.

Instances are created with the actual SQLite3 database path (plus the mode
argument, to select online/offline).

In online mode, a root kw argument is accepted, so that the correct exclude
patterns are returned to make database files invisible to the tree. This
defaults to the directory containing the database file.

All databases are EXCLUSIVE lock, i.e. lock on read.

Raise PropDBError if something goes wrong.

TODO DROP vs TRUNCATE
"""

import os
import sqlite3
import abc

from six import integer_types, raise_from

from lnsync_pkg.p23compat import fstr, fstr2str
from lnsync_pkg.p23compat import sql_text_factory as p23_text_factory
from lnsync_pkg.p23compat import sql_text_storer as p23_text_storer

import lnsync_pkg.printutils as pr
from lnsync_pkg.filetree import Metadata
from lnsync_pkg.onlineoffline import OnOffObject
from lnsync_pkg.proptree import PropDBManager, PropDBError

CUR_DB_FORMAT_VERSION = 1

class SQLPropDBManager(OnOffObject):
    """Manage the file property database for FilePropertyTree using SQLite3,
    methods common to online and offline modes.
    """
    _onoff_super = PropDBManager

    def __init__(self, dbpath, mode=None, **kwargs):
        """dbpath is the actual sqlite3 database filename."""
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
        self._enter_count += 1
        if self._enter_count == 1: # First entered.
            sql_db_path = self.dbpath
            if not os.path.isfile(sql_db_path):
                self._create_empty()
            ver = self.which_db_version()
            if ver is None:
                raise PropDBError("unreadable DB at %s" % fstr2str(sql_db_path))
            elif ver < CUR_DB_FORMAT_VERSION:
                msg = "outdated db version=%d at %s" % (ver, fstr2str(sql_db_path))
                raise PropDBError(msg)
            try:
                self._cx = sqlite3.connect(fstr2str(sql_db_path))
                def factory(x):
                    return p23_text_factory(x)
                self._cx.text_factory = factory
            except sqlite3.Error as exc:
                msg = "cannot open DB at %s" % fstr2str(sql_db_path)
                raise_from(PropDBError(msg), exc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._enter_count -= 1
        if self._enter_count == 0:
            if self._cx:
                self._cx.commit()
                self._cx.close()
        return False # Do not suppress any exception.

    def set_prop_metadata(self, file_id, prop_value, metadata):
        cmd = "INSERT INTO prop VALUES (?, ?, ?, ?, ?);"
        cmd_args = (file_id, prop_value,
                    metadata.size, metadata.mtime, metadata.ctime)
        self._cx.cursor().execute(cmd, cmd_args)

    def get_prop_metadata(self, file_id):
        """Return either a (prop_value, metadata) pair or None."""
        cmd = "SELECT value, size, mtime, ctime FROM prop WHERE file_id=?;"
        res = self._cx.execute(cmd, (file_id,)).fetchone()
        if res is not None:
            if not isinstance(res[0], integer_types):
                raise PropDBError("prop value not integer")
            res = (res[0], Metadata(res[1], res[2], res[3]))
        return res

    def _create_empty(self):
        """Create a new SQL database file and close it."""
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
            pr.error(msg)
            raise_from(PropDBError(msg), exc)
        finally:
            if temp_cx is not None:
                temp_cx.commit()
                temp_cx.close()

    @abc.abstractmethod
    def _all_tables(self):
        pass

    def which_db_version(self):
        """Return db version number or None if not recognized."""
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
        finally:
            if sql_cx:
                sql_cx.close()
        return db_ver

    def rm_offline_tree(self):
        """Remove offline tree (even in online mode)."""
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


class SQLPropDBManagerOffline(SQLPropDBManager):
    """Manage an offline database (file properties plus file tree image) for
    FilePropTree using SQLite3.
    """

    def _all_tables(self):
        return SQLPropDBManager._tables_prop \
             + SQLPropDBManagerOffline._tables_offline

    def set_offline_metadata(self, f_id, metadata):
        """Store file metadata to the database."""
        cmd = "INSERT INTO metadata VALUES (?, ?, ?, ?);"
        self._cx.cursor().execute(
            cmd, (f_id, metadata.size, metadata.mtime, metadata.ctime))

    def get_offline_metadata(self, file_id):
        """Return file metadata from database."""
        cmd = "SELECT size, mtime, ctime FROM metadata WHERE file_id=?;"
        size, mtime, ctime = self._cx.execute(cmd, (file_id,)).fetchone()
        return Metadata(size, mtime, ctime)

    def set_dir_content(self, dir_id, obj_basename, obj_id, obj_is_file):
        """Store a dir entry into the database-stores file tree."""
        # Escape single quotes for sqlite3.
        def storer(x):
            x.replace(fstr("'"), fstr("''"))
            return p23_text_storer(x)
        cmd = "INSERT INTO dir_contents VALUES (?, ?, ?, ?);"
        self._cx.cursor().execute(
            cmd, (dir_id, storer(obj_basename), obj_id, obj_is_file))

    def get_dir_contents(self, dir_id):
        """Generate dir entries from the database-stored file tree."""
        cmd = "SELECT obj_basename, obj_id, obj_is_file " + \
              "FROM dir_contents WHERE parent_id=?;"
        cur = self._cx.execute(cmd, (dir_id,))
        for db_rec in cur.fetchall():
#            yield db_rec
            yield (p23_text_factory(db_rec[0]), db_rec[1], db_rec[2]) #TODO FIX HACK

class SQLPropDBManagerOnline(SQLPropDBManager):
    """Manage an online database (read and updated) for FilePropTree using
    SQLite3.
    """
    def __init__(self, dbpath, root_path=None, mode=None, **kwargs):
        self.treeroot = root_path
        super(SQLPropDBManagerOnline, self).__init__(
            dbpath, root=root_path, **kwargs)

    def get_exclude_patterns(self):
        """Exclude the SQLite3 main database and -journal, -wal and other tmp
        files. The database may be located anywhere, even away from the tree
        root."""
        def is_subdir(path, directory):
            "Test if path is under directory, not strict."
            relative = os.path.relpath(path, directory)
            return not relative.startswith(fstr(os.pardir + os.sep))
        if self.treeroot and is_subdir(self.dbpath, self.treeroot):
            relpath = os.path.relpath(self.dbpath, self.treeroot)
            return [fstr("/") + relpath, fstr("/") + relpath + fstr("-*")]
        else:
            return []

    def _all_tables(self):
        return SQLPropDBManager._tables_prop

    def delete_ids(self, file_ids):
        """Remove single file_id, or list or set of file_ids from property
        table.
        """
        if isinstance(file_ids, integer_types):
            file_ids = (file_ids,)
        try:
            del_prop_cmd = "DELETE FROM prop WHERE file_id=?;"
            self._cx.executemany(del_prop_cmd, [(fid,) for fid in file_ids])
        except sqlite3.Error as exc:
            ms = "could not delete ids"
            pr.error(msg)
            raise_from(PropDBError(msg), exc)

    def delete_ids_except(self, file_ids_to_keep):
        """Delete from the db all ids, except those given. Expensive.
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

    def merge_prop_values(self, tgt_db, remap_id_fn=None):
        """Update db at target with prop values from source, overwriting if
        necessary. """
        tgt_cx = tgt_db._cx
        prop_tab = self._tables_prop[0]
        tab_name = prop_tab[0]
        if remap_id_fn is None:
            tgt_cx.cursor().execute("ATTACH ? AS SOURCE;", (self.dbpath,))
            cmd = ("DELETE FROM %s "
                   "WHERE file_id IN (SELECT file_id FROM SOURCE.%s) ;") \
                    % (tab_name, tab_name)
            tgt_cx.cursor().execute(cmd)
            cmd = "INSERT INTO %s SELECT * FROM SOURCE.%s ;" \
                  % (tab_name, tab_name)
            tgt_cx.cursor().execute(cmd)
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
                resout = (remap_id_fn(res[0]), res[1], res[2], res[3], res[4])
                test_cursor.execute(cmd_test_if, (resout[0],))
                if test_cursor.fetchall():
                    test_cursor.execute(cmd_delete, (resout[0],))
                put_cursor.execute(cmd_insert, resout)
        tgt_db.compact()
