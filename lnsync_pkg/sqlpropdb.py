#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Implement an online and offline file property cache databases for FilePropDB
as an SQLite3 datase.

An online database is stored in a file at the root of the FileTree rootdir.

An offline database is stores in a single file located anywhere.

All databases are EXCLUSIVE lock, i.e. lock on read.
"""

import os
import fnmatch
import random
import sqlite3
import lnsync_pkg.printutils as pr
from lnsync_pkg.filetree import Metadata
from lnsync_pkg.onlineoffline import OnOffObject
from lnsync_pkg.proptree import PropDBManager

CUR_DB_FORMAT_VERSION = 1

ONLINE_DB_PREFIX = "prop-"

def set_online_prefix(prefix):
    global ONLINE_DB_PREFIX
    ONLINE_DB_PREFIX = prefix

def pick_db_basename(dir_path, dbprefix):
    """Find or create a unique basename matching <dbprefix>[0-9]*.db in the
    directory. Raise RuntimeError if there are too many files matching the
    database basename pattern or if there are none and the given dir is not
    writable.
    """
    assert os.path.isdir(dir_path), \
            "pick_db_basename: not a directory: %s ." % dir_path
    if dbprefix.endswith(".db"):
        dbprefix = dbprefix[:-3]
    pattern = "%s[0-9]*.db" % dbprefix
    candidates_base = fnmatch.filter(os.listdir(dir_path), pattern)
    if len(candidates_base) == 1:
        db_basename = candidates_base[0]
    elif candidates_base == []:
        if not os.access(dir_path, os.W_OK):
            raise RuntimeError("no write access to %s" % str(dir_path))
        def random_digit_str(ndigit=3):
            """Return a random string of digits of length ndigit."""
            return ("%%0%dd" % ndigit) % random.randint(0, 10**ndigit-1)
        db_basename = "%s%s.db" % (dbprefix, random_digit_str())
    else:
        raise RuntimeError("too many db files in %s" % str(dir_path))
    return db_basename

class SQLPropDBManager(OnOffObject):
    """Manage the file property database for FilePropertyTree using SQLite3,
    common abstract superclass to online and offline databases.
    """
    _onoff_super = PropDBManager

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

    def __init__(self, location, **kwargs):
        """Always called from a subclass. Location is the actual sqlite3 db
        filename."""
        self._cx = None
        self._enter_count = 0
        self.dbpath = location
        super(SQLPropDBManager, self).__init__(location, **kwargs)

    def _all_tables(self):
        return SQLPropDBManager._tables_prop

    def __enter__(self):
        self._enter_count += 1
        if self._enter_count == 1: # First entered.
            sql_db_path = self.dbpath
            if not os.path.isfile(sql_db_path):
                self.create_empty()
            ver = self.which_db_version()
            if ver is None:
                raise RuntimeError("unreadable DB at %s" % sql_db_path)
            elif ver < CUR_DB_FORMAT_VERSION:
                msg = "outdated db version=%d at %s" % (ver, sql_db_path)
                raise RuntimeError(msg)
            try:
                self._cx = sqlite3.connect(sql_db_path)
                def fac(text):
                    return text
                self._cx.text_factory = fac
            except sqlite3.Error:
                pr.error("cannot open DB at %s", sql_db_path)
                raise
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
            if not isinstance(res[0], (int, long)):
                raise RuntimeError("prop value not integer.")
            res = (res[0], Metadata(res[1], res[2], res[3]))
        return res

    def reset_all_tables(self):
        for table_info in self._all_tables():
            SQLPropDBManager._reset_db_table(self._cx, table_info)

    def create_empty(self):
        """Create a new SQL database file and close it."""
        location = self.dbpath
        assert not os.path.exists(location)
        temp_cx = None
        try:
            temp_cx = sqlite3.connect(location)
            SQLPropDBManager._reset_db_tables(temp_cx, self._all_tables())
#            self.reset_all_tables()
            temp_cx.execute(
                "PRAGMA user_version=%d;" % int(CUR_DB_FORMAT_VERSION))
            temp_cx.execute(
                "PRAGMA locking_mode=EXCLUSIVE;")
        except sqlite3.Error:
            pr.error("cannot create database at %s", location)
            raise
        finally:
            if temp_cx is not None:
                temp_cx.close()

    def which_db_version(self):
        """Return db version number or None if not recognized."""
        db_path = self.dbpath
        sql_cx = None
        db_ver = None
        try:
            sql_cx = sqlite3.connect(db_path)
            db_ver_rec = sql_cx.execute("PRAGMA user_version;").fetchone()
            if db_ver_rec is not None:
                db_ver = db_ver_rec[0]
            else:
                db_ver = 0
        finally:
            if sql_cx:
                sql_cx.close()
        return db_ver

    def reset_offline_tree(self):
        SQLPropDBManager._reset_db_tables(self._cx, self._tables_offline)
        self._cx.execute("PRAGMA foreign_keys = ON;")
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

    @staticmethod
    def mode_from_location(location, mandatory_mode=None):
        """Given a location path, decide if it could be an offline database or
        just a root dir or can't tell (eg empty path)."""
        if mandatory_mode is None:
            if os.path.isdir(location):
                mandatory_mode = "online"
            elif os.path.isfile(location):
                mandatory_mode = "offline"
            else:
                raise ValueError("expected file or dir: %s" % (location,))
        if mandatory_mode == "online":
            if not os.path.isdir(location):
                raise ValueError("expected db rootdir: %s" % (location,))
            elif not os.access(location, os.R_OK):
                raise RuntimeError("cannot read from: %s" % (location,))
            mode = "online"
        elif mandatory_mode == "offline":
            if os.path.exists(location):
                if not os.path.isfile(location):
                    raise ValueError("expected db file: %s" % (location,))
                elif not os.access(location, os.R_OK):
                    raise RuntimeError("cannot read from: %s" % (location,))
            mode = "offline"
        return mode

    def commit(self):
        self._cx.commit()

    def compact(self):
        self._cx.execute("VACUUM;")


class SQLPropDBManagerOffline(SQLPropDBManager):
    """Manage an offline database (file properties plus file tree image) for
    FilePropTree using SQLite3.
    """

#    def __init__(self, location, **kwargs):
#        super(SQLPropDBManagerOffline, self).__init__(location, **kwargs)
#
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
        obj_basename = obj_basename.replace("'", "''")
        cmd = "INSERT INTO dir_contents VALUES (?, ?, ?, ?);"
        self._cx.cursor().execute(
            cmd, (dir_id, obj_basename, obj_id, obj_is_file))

    def get_dir_contents(self, dir_id):
        """Generate dir entries from the database-stored file tree."""
        cmd = "SELECT obj_basename, obj_id, obj_is_file " + \
              "FROM dir_contents WHERE parent_id=?;"
        cur = self._cx.execute(cmd, (dir_id,))
        for db_rec in cur.fetchall():
            yield db_rec

class SQLPropDBManagerOnline(SQLPropDBManager):
    """Manage an online database (read and updated) for FilePropTree using
    SQLite3.
    """
    def __init__(self, location, *args, **kwargs):
        prefix = kwargs.get("prefix", ONLINE_DB_PREFIX)
        location = os.path.join(location, pick_db_basename(location, prefix))
        super(SQLPropDBManagerOnline, self).__init__(location, *args, **kwargs)

    def get_exclude_patterns(self):
        return ["/"+os.path.basename(self.dbpath)]

    def delete_ids(self, file_ids):
        """Remove single file_id, or list or set of file_ids from property
        table.
        """
        if isinstance(file_ids, (long, int)):
            file_ids = (file_ids,)
        try:
            del_prop_cmd = "DELETE FROM prop WHERE file_id=?;"
            self._cx.executemany(del_prop_cmd, [(fid,) for fid in file_ids])
        except Exception:
            pr.error("could not delete ids")
            raise

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

    def copy_prop_values(self, tgt_db, remap_fn=None):
        """Copy prop values to target db."""
        tgt_cx = tgt_db._cx
        if remap_fn is None:
            tgt_cx.cursor().execute("ATTACH ? AS SOURCE;", (self.dbpath,))
            for tab in self._all_tables():
                cmd = "INSERT INTO %s SELECT * FROM SOURCE.%s ;" % (tab[0], tab[0])
                tgt_cx.cursor().execute(cmd)
            tgt_db.compact()
        else:
            raise NotImplementedError("no remapping on copying a db yet")

    def merge_prop_values(self, tgt_db, remap_fn=None):
        """Update db at target with prop values from source, overwriting if
        necessary."""
        tgt_cx = tgt_db._cx
        if remap_fn is None:
            tgt_cx.cursor().execute("ATTACH ? AS SOURCE;", (self.dbpath,))
            for tab in self._all_tables():
                cmd = ("DELETE FROM %s "
                       "WHERE file_id IN (SELECT file_id FROM SOURCE.%s) ;") \
                        % (tab[0], tab[0])
                tgt_cx.cursor().execute(cmd)
            for tab in self._all_tables():
                cmd = "INSERT INTO %s SELECT * FROM SOURCE.%s ;" % (tab[0], tab[0])
                tgt_cx.cursor().execute(cmd)
            tgt_db.compact()
        else:
            raise NotImplementedError("no remapping on merging a db yet")
