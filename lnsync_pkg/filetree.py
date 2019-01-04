#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""A representation of a source file tree with support for hardlinks.
Each object is either a file or a directory. Because file hardlinks
are supported, files are distinct from file paths.  All paths are
relative to the root.

Each file is assigned a short id number. Files are indexed by file id.
Each directory is also assigned an id number; the root id is zero.

The source tree is scanned from the source file tree one directory
at a time, so the representation is built as needed. Each directory
is scanned at most once. Some files in the source tree may be ignored
when scanning (i.e. do not assume that if a path does not exist
in the data structure implies that the same path is absent in the
source tree).

An interface is provided for walking entries in a directory or
in a whole subtree.

Methods are provided for adding and removing specific files and paths,
writing them back to the source tree. These should only be used on
directories which have been marked as scanned.

(Private methods _add_path and _rm_path are for internal use only.)

Optionally, file metadata (size, mtime, and ctime) is also read and
recorded. In this case, files are also indexed by size.

By default, the tree is read from a mounted file system. Only readable
files and read/exec directories are read as such. Other files, dirs,
symlinks and special files are ignored and read as 'other object'
occupying a basename, but skipped on walk generators, etc.
File ownership is ignored.

Use the fileid module to obtain file id. (inode is used if the source
tree is on a file system supporting a persistent inode number, e.g. ext3.)

Files and directories are represented by FileObj and DirObj instances.
More complex information may be stored for a file by subclassing FileObj
and setting _file_type.

Support is provided for path creation/deleting commands: mv, ln, rm.
All commands are reversible, except those that delete the final path
for a file. Supports is provided for reversing a sequence of reversible
commands.
"""

from __future__ import print_function

import os

import lnsync_pkg.printutils as pr
from lnsync_pkg.fileid import make_id_computer

class TreeObj(object):
    "Abstract base class for all tree objects."
    def is_dir(self):
        return False
    def is_file(self):
        return False

class OtherObj(TreeObj):
    pass

class FileObj(TreeObj):
    """File info: id, relpaths, metadata.
    """
    __slots__ = "file_id", "file_metadata", "relpaths"
    def __init__(self, file_id, metadata):
        self.file_id = file_id
        self.file_metadata = metadata
        self.relpaths = []
    def is_file(self):
        return True

class DirObj(TreeObj):
    """Dir hold lists of files and subdirs contained, own basename, parent dir.
    """
    __slots__ = "dir_id", "parent", "entries", "relpath", "scanned"
    def __init__(self, dir_id):
        self.dir_id = dir_id
        self.parent = None # A dir object.
        self.entries = {} # A map basename->obj.
        self.relpath = None # Cache.
        self.scanned = False
    def was_scanned(self):
        """Return True if this directory was scanned."""
        return self.scanned
    def mark_scanned(self):
        """Mark this dir as being scanned or fully scanned."""
        self.scanned = True
    def add_entry(self, basename, obj):
        assert not basename in self.entries, "add_entry: basename already in dir."
        self.entries[basename] = obj
        if obj.is_dir():
            obj.parent = self
    def rm_entry(self, basename):
        assert basename in self.entries, "rm_entry: '%s' not in dir." % (basename,)
        obj = self.entries[basename]
        del self.entries[basename]
        if obj.is_dir():
            obj.parent = None
    def get_entry(self, bname):
        if bname in self.entries:
            return self.entries[bname]
        else:
            return None
    def iter_subdirs(self):
        assert self.was_scanned(), "trying to iterate unscanned dir"
        for obj in self.entries.itervalues():
            if obj.is_dir():
                yield obj
    def get_relpath(self):
        """Return the dir's relpath, without a trailing separator.
        """
        if self.relpath is None:
            curr_dir = self
            path = None
            while curr_dir.parent is not None:
                for entryname, obj in curr_dir.parent.entries.iteritems():
                    if obj is curr_dir:
                        if path is None:
                            path = entryname
                        else:
                            path = os.path.join(entryname, path)
                        break
                curr_dir = curr_dir.parent
            if path is None:
                path = ""
            self.relpath = path
        return self.relpath
    def is_dir(self):
        return True

class Metadata(object):
    """File metadata: size, mtime, and ctime."""
    __slots__ = "size", "mtime", "ctime"
    __hash__ = None # Since we redefine eq, declare objects not hashable.
    def __init__(self, size, mtime, ctime):
        self.size = size
        self.mtime = mtime
        self.ctime = ctime
    def __eq__(self, other):
        return self.size == other.size and self.mtime == other.mtime
    def __str__(self):
        return "[md sz:%d mt:%d ct:%d]" % (self.size, self.mtime, self.ctime)

class FileTree(object):
    """Represent a disk file tree with hardlinks (multiple file paths per file).

    Index files by size and serial number.
    """
    def __init__(self, root_path, maxsize=None, use_metadata=False, writeback=True):
        assert use_metadata or maxsize is None, "FileTree: maxsize set on no use_metadata."
        self._maxsize = maxsize
        self._use_metadata = use_metadata
        self._file_type = FileObj
        self.rootdir_path = root_path
        self._next_free_dir_id = 1
        self.rootdir_obj = self._make_dir(0)
        self._size_to_files = {} # Available only once the full tree has been scanned.
        self._size_to_files_ready = False
        self._id_to_file = {}    # May be filled on-demand.
        self._id_computer = make_id_computer(root_path)
        self.writeback = writeback

    def __enter__(self):
        if not os.path.isdir(self.rootdir_path):
            raise RuntimeError("FileTree: not a dir: %s." % (self.rootdir_path,))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False # Do not suppress any exception.

    def _make_dir(self, dir_id=None):
        """Create and return a new, properly numbered DirObj instance.
        Its basename will be given when it is inserted into a parent directory.
        """
        if dir_id is None:
            dir_id = self._next_free_dir_id
            self._next_free_dir_id += 1
        dir_obj = DirObj(dir_id)
        return dir_obj

    def scan_dir(self, dir_obj, skipbasenames=None):
        """Scan a directory from source, if it hasn't been scanned before.

        Yield those dir entries which are files.
        """
        assert dir_obj is not None, "scan_dir: no dir_obj"
        if dir_obj.was_scanned():
            return
        for (basename, obj_id, obj_type, raw_metadata) in \
                self._gen_source_dir_entries(dir_obj, skipbasenames=skipbasenames):
            try:
                basename.decode('utf-8')
            except Exception:
                msg = "not a valid utf-8 filename: '%s'. proceeding."
                msg = msg % (os.path.join(dir_obj.get_relpath(), basename),)
                pr.warning(msg)
            if obj_type == FileObj:
                if obj_id in self._id_to_file:
                    file_obj = self._id_to_file[obj_id]
                else:
                    file_obj = self.new_file_obj(obj_id, raw_metadata)
                    if self._use_metadata and self._maxsize is not None \
                            and file_obj.file_metadata.size > self._maxsize:
                        obj_abspath = self.rel_to_abs(os.path.join(dir_obj.get_relpath(), basename))
                        pr.warning("ignored large file '%s'" % obj_abspath)
                        continue
                self._add_path(file_obj, dir_obj, basename)
            elif obj_type == DirObj:
                self._next_free_dir_id = max(self._next_free_dir_id, obj_id + 1)
                dir_obj.add_entry(basename, self._make_dir(obj_id))
            else:
                dir_obj.add_entry(basename, OtherObj())
        dir_obj.mark_scanned()

    def scan_full_tree(self, start_dir=None):
        """Scan full subtree rooted at start_dir, meaning recursively scan all
        subdirectories not yet scanned.
        """
        if start_dir is None:
            start_dir = self.rootdir_obj
        self.scan_dir(start_dir)
        for obj in start_dir.entries.values():
            if obj.is_dir() and not obj.was_scanned():
                self.scan_dir(obj)
                self.scan_full_tree(obj)
        if start_dir == self.rootdir_obj:
            self._size_to_files_ready = True

    def _gen_source_dir_entries(self, dir_obj, skipbasenames=None):
        """Generate source entries for a directory.

        Yield (basename, obj_id, obj_type, rawmetadata), where
        rawmetadata is some data that may passed on to new_file_obj
        to help generate file metadata.
        obj_type is one of DirObj, FileObj or OtherObj.

        This default version reads the source file tree.
        It may be overridden, perhaps along new_file_obj and other methods.
        This is called while scanning a directory.
        (The directory may already have been as scanned.)
        """
        dir_relpath = dir_obj.get_relpath()
        dir_abspath = self.rel_to_abs(dir_relpath)
        for obj_bname in os.listdir(dir_abspath):
            obj_abspath = os.path.join(dir_abspath, obj_bname)
            if skipbasenames is not None and obj_bname in skipbasenames:
                pr.warning("ignored %s" % obj_abspath)
                yield (obj_bname, None, OtherObj, None)
            elif os.path.islink(obj_abspath):
                pr.warning("ignored softlink %s" % obj_abspath)
                yield (obj_bname, None, OtherObj, None)
            elif os.path.isdir(obj_abspath):
                if not os.access(obj_abspath, os.R_OK + os.X_OK):
                    pr.warning("ignored no-rx-access dir %s" % obj_abspath)
                    yield (obj_bname, None, OtherObj, None)
                else:
                    dir_id = self._next_free_dir_id
                    self._next_free_dir_id += 1
                    yield (obj_bname, dir_id, DirObj, None)
            elif os.path.isfile(obj_abspath):
                if not os.access(obj_abspath, os.R_OK):
                    pr.warning("ignored no-read-access file %s" % obj_abspath)
                    yield (obj_bname, None, OtherObj, None)
                else:
                    obj_relpath = os.path.join(dir_relpath, obj_bname)
                    pr.progress("%s" % obj_relpath)
                    stat_data = os.stat(obj_abspath)
                    fid = self._id_computer.get_id(obj_relpath, stat_data)
                    yield (obj_bname, fid, FileObj, stat_data)
            else:
                pr.warning("ignored special file %s" % obj_abspath)
                yield (obj_bname, None, OtherObj, None)


    def new_file_obj(self, obj_id, rawmetadata):
        """Create and return a new file object, from id and rawmetadata.

        If self._use_metadata is false, then no metadata is created.
        In the default version, rawmetadata is os.stat data, with inode.
        """
        stat_data = rawmetadata
        if self._use_metadata:
            metadata = Metadata(stat_data.st_size,
                                int(stat_data.st_mtime),
                                int(stat_data.st_ctime))
        else:
            metadata = None
        file_obj = self._file_type(obj_id, metadata)
        return file_obj

    def size_to_files(self, size=None):
        """Return either _size_to_files hash (if size is None) or an entry.

        If _size_to_files might not contain the full index, do a full-tree scan.
        """
        assert self._use_metadata, "size_to_files without metadata."
        if not self._size_to_files_ready:
            self.scan_full_tree()
        if size is None:
            return self._size_to_files
        else:
            return self._size_to_files[size]

    def _add_path(self, file_obj, dir_obj, fbasename):
        """Add a new path for a file object, with fbasename at an existing dir.

        If this is the first path, the file is inserted into
        the tree data structures.
        """
        if file_obj.relpaths == []:
            fid = file_obj.file_id
            self._id_to_file[fid] = file_obj
            if self._use_metadata:
                f_size = file_obj.file_metadata.size
                if f_size in self._size_to_files:
                    self._size_to_files[f_size].append(file_obj)
                else:
                    self._size_to_files[f_size] = [file_obj]
        dir_obj.add_entry(fbasename, file_obj)
        relpath = os.path.join(dir_obj.get_relpath(), fbasename)
        file_obj.relpaths.append(relpath)

    def _rm_path(self, file_obj, dir_obj, fbasename):
        """Remove an existing path for an existing file, with dirname
        at an existing dir, which has already been scanned in.

        If the last path for a file is removed, the file is removed
        from all tree data structure, ie from the size and file_id
        hashes.
        """
        dir_obj.rm_entry(fbasename)
        relpath = os.path.join(dir_obj.get_relpath(), fbasename)
        assert relpath in file_obj.relpaths, "_rm_path: non-existing relpath."
        file_obj.relpaths.remove(relpath)
        if file_obj.relpaths == []:
            fid = file_obj.file_id
            del self._id_to_file[fid]
            if self._use_metadata:
                file_sz = file_obj.file_metadata.size
                self._size_to_files[file_sz].remove(file_obj)
                if self._size_to_files[file_sz] == []:
                    del self._size_to_files[file_sz]

    def _rm_file(self, file_obj):
        """Remove a file, i.e. remove all paths."""
        paths = list(file_obj.relpaths) # Copy before modifying.
        for path in paths:
            tr_obj = self.follow_path(os.path.dirname(path))
            self._rm_path(file_obj, tr_obj, os.path.basename(path))

    def follow_path(self, relpath):
        """Return file or dir or other object by relpath from root, or None.

        Does not follow symlinks.
        """
        assert self.rootdir_obj is not None, "follow_path: no rootdir_obj."
        curdir_obj = self.rootdir_obj
        if relpath == "." or relpath == "":
            return curdir_obj
        components = relpath.split(os.sep)
        while components and curdir_obj:
            comp = components[0]
            components = components[1:]
            if curdir_obj.is_dir() and not curdir_obj.was_scanned():
                self.scan_dir(curdir_obj)
            next_obj = curdir_obj.get_entry(comp)
            if next_obj is None or next_obj.is_dir() or not components:
                curdir_obj = next_obj
            else:
                curdir_obj = None
        return curdir_obj

    def walk_dir_contents(self, subdir_path, dirs=False):
        """Yield (obj, basename) for entries (files and possibly dirs) at
        subdir_path (a relative path).

        Yield only files and possibly directories, skipping other objects.
        """
        assert self.rootdir_obj is not None, "walk_dir_contents: no rootdir_obj."
        subdir = self.follow_path(subdir_path)
        assert subdir is not None and subdir.is_dir()
        self.scan_dir(subdir)
        for basename, obj in subdir.entries.iteritems():
            if obj.is_file() or (dirs and obj.is_dir()):
                yield obj, basename

    def walk_paths(self, startdir_path="", recurse=True, dirs=False, files=True, topdown=True):
        """Generate (obj, parent_obj, relpath) for distinct relpaths,
        not necessarily distinct tree objects.

        Always skip startdir.
        If dirs is True, include other dirs.
        If files is True, include files.
        If recurse is True, walk the full tree, else just subdir immediate contents.
        If topdown is True
        Always skip other objects.
        """
        assert self.rootdir_obj is not None, "walk_paths: no rootdir_obj."
        startdir_obj = self.follow_path(startdir_path)
        assert startdir_obj is not None and startdir_obj.is_dir(), "walk_paths: dobj not a dir."
        def output_files(dir_obj):
            dir_path = dir_obj.get_relpath()
            for basename, obj in dir_obj.entries.iteritems():
                if (files and obj.is_file()):
                    yield obj, dir_obj, os.path.join(dir_path, basename)
        def output_dir(dir_obj):
            if dirs and dir_obj != startdir_obj:
                dir_path = dir_obj.get_relpath()
                yield dir_obj, dir_obj.parent, dir_path
        if not recurse:
            for k in output_files(startdir_obj):
                yield k
            for subd_obj in startdir_obj.iter_subdirs():
                for k in output_dir(subd_obj):
                    yield k
        elif topdown:
            stack = [startdir_obj]
            while stack:
                curdir_obj = stack.pop()
                self.scan_dir(curdir_obj)
                for subd_obj in curdir_obj.iter_subdirs():
                    stack.append(subd_obj)
                for k in output_dir(curdir_obj):
                    yield k
                for k in output_files(curdir_obj):
                    yield k
        else:
            stack = [[None], [startdir_obj]]
            while stack: # stack[-2:0]=[dirs, unprocessed child_dirs of dirs[-1]].
                prevdir_children = stack[-1]
                if prevdir_children:
                    nextdir = prevdir_children[-1]
                    self.scan_dir(nextdir)
                    nextdir_children = list(nextdir.iter_subdirs())
                    stack.append(nextdir_children)
                else:
                    stack.pop()
                    prevdir = stack[-1].pop()
                    if prevdir is None:
                        break
                    for k in output_files(prevdir):
                        yield k
                    for k in output_dir(prevdir):
                        yield k

    def id_to_file(self, fid):
        assert fid in self._id_to_file, "id_to_file: unknown fid %d." % fid
        return self._id_to_file[fid]

    def rel_to_abs(self, rel_path):
        """Prepend the root dir to the path name (file or dir).
        """
        return os.path.join(self.rootdir_path, rel_path)

    def abs_to_rel(self, abs_path):
        """Strip off the root dir from the path name (file or dir).
        """
        return os.path.relpath(abs_path, self.rootdir_path)

    def get_all_sizes(self):
        """Return a set of all file sizes in the tree.
        """
        # Obtain the full, up-to-date size to files hash table.
        sz__to_files_map = self.size_to_files()
        return set(sz__to_files_map.keys())

    def _create_dir_if_needed_writeback(self, dir_relpath):
        """Return dir obj corresponding to dir_relpath, creating all needed directories.

        Raise RuntimeError or OSError if directory cannot be created."""
        tr_obj = self.follow_path(dir_relpath)
        if tr_obj is None:
            supdname = os.path.dirname(dir_relpath)
            dbasename = os.path.basename(dir_relpath)
            supd = self._create_dir_if_needed_writeback(supdname)
            newd = self._make_dir(None)
            newd.mark_scanned()
            supd.add_entry(dbasename, newd)
            if self.writeback:
                os.mkdir(self.rel_to_abs(dir_relpath))
            return newd
        elif tr_obj.is_dir():
            return tr_obj
        else:
            raise RuntimeError("cannot create dir at '%s'." % (dir_relpath,))

    def add_path_writeback(self, file_obj, relpath):
        """Add a new hardlink to an existing file, creating intermediate dirs if needed.
        """
        tr_obj = self._create_dir_if_needed_writeback(os.path.dirname(relpath))
        self._add_path(file_obj, tr_obj, os.path.basename(relpath))
        if self.writeback:
            assert file_obj is not None, "add_path_writeback: no file_obj."
            assert file_obj.relpaths, "add_path_writeback: some path must exist."
            os.link(self.rel_to_abs(file_obj.relpaths[0]),\
                    self.rel_to_abs(relpath))

    def rm_path_writeback(self, file_obj, relpath):
        if self.writeback:
            os.unlink(self.rel_to_abs(relpath))
        tr_obj = self.follow_path(os.path.dirname(relpath))
        assert tr_obj is not None and tr_obj.is_dir(), \
            "rm_path_writeback: expected a dir at '%s'." % (os.path.dirname(relpath),)
        self._rm_path(file_obj, tr_obj, os.path.basename(relpath))

    def mv_path_writeback(self, file_obj, fn_from, fn_to):
        """Rename one of the file's paths.
        """
        # Cannot be achieved by a combination of adding/removing links
        # on filesystems not supporting hardlinks.
        d_from = self.follow_path(os.path.dirname(fn_from))
        assert d_from is not None and d_from.is_dir(), \
            "mv_path_writeback: expected a dir at '%s'." % (os.path.dirname(fn_from),)
        d_to = self._create_dir_if_needed_writeback(os.path.dirname(fn_to))
        self._add_path(file_obj, d_to, os.path.basename(fn_to))
        self._rm_path(file_obj, d_from, os.path.basename(fn_from))
        if self.writeback:
            os.rename(self.rel_to_abs(fn_from), self.rel_to_abs(fn_to))

    def rm_dir_writeback(self, dir_obj):
        assert not dir_obj.entries, "trying to remove non-empty dir."
        assert dir_obj.parent, "trying to remove rootdir,"
        relpath = dir_obj.get_relpath()
        basename = os.path.basename(relpath)
        dir_obj.parent.rm_entry(basename)
        if self.writeback:
            os.rmdir(self.rel_to_abs(relpath))

    def exec_cmds(self, cmds):
        for command in cmds:
            self.exec_cmd(command)

    def exec_cmd(self, cmd):
        "Execute a command (which_cmd, arg1, arg2)."
        ctype, fn_from, fn_to = cmd
        obj_from = self.follow_path(fn_from)
        assert obj_from is not None and obj_from.is_file(), \
            "exec_cmd: expected a file at '%s'." % (fn_from,)
        if fn_to is not None:
            obj_to = self.follow_path(fn_to)
        else:
            obj_to = None
        if ctype == "mv":
            assert obj_to is None, "exec_cmd: no obj_to."
            self.mv_path_writeback(obj_from, fn_from, fn_to)
        elif ctype == "ln":
            assert obj_to is None, "exec_cmd: no obj_to."
            self.add_path_writeback(obj_from, fn_to,)
        elif ctype == "rm":
            self.rm_path_writeback(obj_from, fn_from)
        else:
            raise RuntimeError("exec_cmd: unknown command %s" % (cmd,))

    def exec_cmd_reverse(self, cmd):
        "Execute in reverse a command (which_cmd, arg1, arg2)."
        assert len(cmd) == 3, "exec_cmd_reverse: bad cmd: %s" % (cmd,)
        ctype, fn_from, fn_to = cmd
        if ctype == "mv":
            self.exec_cmd(("mv", fn_to, fn_from))
        elif ctype == "ln":
            self.exec_cmd(("rm", fn_to, fn_from)) # Remove link, retain witness.
        elif ctype == "rm":
            witness_obj = self.follow_path(fn_to)
            if witness_obj is None or not witness_obj.is_file():
                raise RuntimeError("exec_cmd_reverse: cannot undo this rm cmd.")
            self.exec_cmd(("ln", fn_to, fn_from)) # Recover link from witness.
        else:
            raise RuntimeError("exec_cmd_reverse: unknown command %s." % (cmd,))

    def exec_cmds_reverse(self, cmds):
        """Assuming a list of commands has been executed, reverse it by
        undoing each in reverse order.
        """
        for cmd in reversed(cmds):
            self.exec_cmd_reverse(cmd)

    def __str__(self):
        return "%s(%s)" % (object.__str__(self), self.rootdir_path)
