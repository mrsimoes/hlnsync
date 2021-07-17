# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Implement an SQLite3 database suited  to FileProp, with ONLINE and OFFLINE
modes.

Prop values are SQLite3 INT values, meaning int64 (signed).

In offline mode, it stores a file tree structure as well as file metadata.

Tree structure information includes dirnames and filenames. These are
stored in raw encoding, decoded from the surrogate-escaped Unicode,
binary equal the names in the file system.

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

user_version, 31 bit non-negative integer

"""

# pylint: disable=unused-import # Fails to detect metaclass parameters.

import os
import sqlite3
import abc
import sys
from enum import IntEnum

from lnsync_pkg.miscutils import BitField, is_subdir, int32_to_uint32, uint32_to_int32
import lnsync_pkg.printutils as pr
from lnsync_pkg.glob_matcher import ExcludePattern
from lnsync_pkg.filetree import Metadata
from lnsync_pkg.modaltype import ONLINE, OFFLINE
from lnsync_pkg.propdbmanager import PropDBManager, PropDBError, PropDBNoValue
from lnsync_pkg.blockhash import BlockHasher, HasherAlgo

# From str (surrogates escaped) to db (binary) value and back.
FSE = sys.getfilesystemencoding() # Always UTF8 on Linux.
def _SQL_TEXT_FACTORY(stored_bin):
    return stored_bin.decode(FSE, "surrogateescape")
def _SQL_TEXT_STORER(string):
    return string.encode(FSE, "surrogateescape")

#def mk_online_db(dir_path, db_basename):
#    return SQLHashDBManager(os.path.join(dir_path, db_basename), mode=ONLINE)

class HasherFunction(IntEnum):
    """
    Enum which mathematical hashing function.
    """
    XXHASH32 = 0
    XXHASH64 = 1
    CUSTOM = 2

    @classmethod
    def from_current_hasher_algo(cls):
        return cls.from_hasher_algo(BlockHasher.get_algo())

    @classmethod
    def from_hasher_algo(cls, hasher_algo):
        table = {HasherAlgo.PYHASHXX: cls.XXHASH32,
                 HasherAlgo.XXHASH32: cls.XXHASH32,
                 HasherAlgo.XXHASH64: cls.XXHASH64,
                 HasherAlgo.CUSTOM:   cls.CUSTOM,
                 }
        if not hasher_algo in table:
            raise RuntimeError("Unknown blockhash algorithm:", hasher_algo)
        return table[hasher_algo]

class UserVersion(BitField):
    """
    SQLite user_version field, a 31 bit non-negative integer, as bit struct:
    Bits 0-3: database version (currently  1)
    Bits 4-7: Hasher function used in this database.
        (Default value 0 is XXHASH32 for backwards compatibility.)
    """
    CUR_DB_FORMAT_VERSION = 1
    def __init__(self, value=0):
        super().__init__(value)
        self.get_hasher_function() # Test for valid value.
    def get_db_version(self):
        return self[0:4]
    def set_db_version(self, db_version=None):
        if db_version is None:
            db_version = self.CUR_DB_FORMAT_VERSION
        self[0:4] = db_version
    def get_hasher_function(self):
        """
        May raise an exception if the code does not match any hasher.
        """
        hasher_funcion = HasherFunction(self[4:8])
        return hasher_funcion
    def set_hasher_function(self, hasher_function):
        self[4:8] = hasher_function
    def get_raw_value(self):
        return self._d

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
        super().__init__(dbpath, **kwargs)

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
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False # Do not suppress any exception.

    def open(self):
        sql_db_path = self.dbpath
        if sql_db_path in self._current_online_cx:
            cx_info = self._current_online_cx[sql_db_path]
            cx_info[0] += 1
            self._cx = cx_info[1]
        else:
            if not os.path.isfile(sql_db_path):
                self._create_empty()
            self._check_user_version()
            try:
                self._cx = sqlite3.connect(sql_db_path)
            except sqlite3.Error as exc:
                msg = "cannot open DB at " + sql_db_path
                raise PropDBError(msg) from exc
            self._current_online_cx[sql_db_path] = [1, self._cx]
        return self

    def close(self):
        if self._cx:
            sql_db_path = self.dbpath
            if sql_db_path in self._current_online_cx:
                cx_info = self._current_online_cx[sql_db_path]
                cx_info[0] -= 1
                if cx_info[0] == 0:
                    del self._current_online_cx[sql_db_path]
                    self._cx.commit()
                    self._cx.close()

    @abc.abstractmethod
    def _all_tables(self):
        pass

    def set_prop_metadata(self, file_id, prop_value, metadata):
        cmd = "INSERT INTO prop VALUES (?, ?, ?, ?, ?);"
        cmd_args = (file_id, prop_value,
                    metadata.size, metadata.mtime, metadata.ctime)
        try:
            self._cx.execute(cmd, cmd_args)
        except Exception as exc:
            msg = "setting metadata of: " + self.dbpath
            msg += ": " + str(exc)
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
            raise PropDBError("reading: " + self.dbpath) from exc
        if res is None:
            raise PropDBNoValue("no value for file id %d at DB %s" % \
                    (file_id, self.dbpath))
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
        pr.progress("Creating new database %s ..." % (dbpath,))
        temp_cx = None
        try:
            temp_cx = sqlite3.connect(dbpath)
            SQLPropDBManager._reset_db_tables(temp_cx, self._all_tables())
            self._set_user_version(temp_cx)
            temp_cx.execute("PRAGMA locking_mode=EXCLUSIVE;")
            temp_cx.execute("PRAGMA foreign_keys=ON;")
        except sqlite3.Error as exc:
            msg = "cannot create database at " + dbpath
            raise PropDBError(msg) from exc
        finally:
            if temp_cx is not None:
                temp_cx.commit()
                temp_cx.close()

    def _set_user_version(self, cx):
        """
        Stamp a new database.
        """
        user_version = UserVersion()
        user_version.set_db_version()
        hasher_function = HasherFunction.from_current_hasher_algo()
        user_version.set_hasher_function(hasher_function)
        user_version = user_version.get_raw_value()
        cx.execute("PRAGMA user_version=%d;" % user_version)

    def _get_user_version(self):
        """
        Return user version number or raise PropDBError if it cannot be read.
        """
        sql_cx = None
        try:
            sql_cx = sqlite3.connect(self.dbpath)
            user_ver_rec = sql_cx.execute("PRAGMA user_version;").fetchone()
            if user_ver_rec is None:
                raise PropDBError("could not read version")
            user_ver = user_ver_rec[0]
            if user_ver < 0:
                raise PropDBError("negative user_version")
            try:
                user_ver = UserVersion(user_ver)
            except Exception:
                raise PropDBError("invalid user_version")
        except sqlite3.Error as exc:
            msg = "cannot open DB at " + self.dbpath
            raise PropDBError(msg) from exc
        finally:
            if sql_cx:
                sql_cx.close()
        return user_ver

    def _check_user_version(self):
        ver = self._get_user_version()
        if ver is None:
            raise PropDBError("unreadable DB at " + self.dbpath)
        db_ver = ver.get_db_version()
        try:
            db_hasher_func = ver.get_hasher_function()
        except Exception as exc: # TODO: be more specific
            msg = "unknown hasher function for %s" % (self.dbpath,)
            raise PropDBError(msg) from exc
        curr_hasher_func = HasherFunction.from_current_hasher_algo()
        pr.info("Checking:", db_hasher_func, curr_hasher_func)
        if db_hasher_func != curr_hasher_func:
            errstr = "incompatible hash functions: %s and %s" % \
                     (str(db_hasher_func), str(curr_hasher_func))
            raise PropDBError(errstr)
        if db_ver < UserVersion.CUR_DB_FORMAT_VERSION:
            msg = "update old database format=%d at %s" \
                  % (ver, self.dbpath)
            raise PropDBError(msg)
        elif db_ver > UserVersion.CUR_DB_FORMAT_VERSION:
            msg = "cannot handle new database format=%d at %s" \
                  % (ver, self.dbpath)
            raise PropDBError(msg)

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
            cmd = "CREATE INDEX %sidx ON %s (%s);"
            sql_cx.execute(cmd % (tab_name, tab_name, index))
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
                   "WHERE file_id IN (SELECT file_id FROM SOURCE.%s) ;")
            tgt_cx.execute(cmd % (tab_name, tab_name))
            cmd = "INSERT INTO %s SELECT * FROM SOURCE.%s ;"
            tgt_cx.execute(cmd % (tab_name, tab_name))
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
        super().__enter__()
        try:
            _res = list(self.get_dir_entries(0)) # Force evaluation.
        except Exception as exc:
            self.__exit__(*sys.exc_info())
            msg = "not an offline database, was it created with mkoffline? "
            raise PropDBError(msg + self.dbpath) from exc
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
                % (self.dbpath, file_id)
            raise PropDBError(msg) from exc
        return Metadata(size, mtime, ctime)

    def put_dir_entry(self, dir_id, obj_basename, obj_id, obj_is_file):
        """
        Store a dir entry into the database-stores file tree.
        """
        # Escape single quotes for sqlite3.
        def storer(string):
            string.replace("'", "''")
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
    def __init__(self, dbpath, topdir_path=None, **kwargs):
        self.treeroot = topdir_path
        super().__init__(dbpath, root=topdir_path, **kwargs)

    def get_glob_patterns(self):
        """
        Exclude the SQLite3 main database and -journal, -wal and other tmp
        files. The database may be located anywhere, even away from the tree
        root.
        """
        if self.treeroot and is_subdir(self.dbpath, self.treeroot):
            relpath = os.path.relpath(self.dbpath, self.treeroot)
            return [ExcludePattern("/" + relpath),
                    ExcludePattern("/" + relpath + "-*")]
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
                (self.dbpath, file_ids, exc)
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
