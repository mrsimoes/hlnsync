#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Manage a file tree in a mounted filesystem, with support for hardlinks.

A file tree item is either a file or a directory or catch-all "other"
item.

Paths relative to the tree root are used throughout. Because file
hardlinks are supported, files are distinct from file paths. Each file
has one or more file paths.

File are assigned a file id (a persistent serial number, e.g. inode)
and are accessible by file id. Directories are also assigned ids.
The root has id zero.

An in-memory representation is built by scanning the disk file tree.
Scanning is done a need-to basis and scanned directories are marked.
While scanning, items of the disk tree may be ignored using glob patters.

Only readable files and read/exec directories are read as files and
directories. Other files, other dirs, all symlinks and special files are
read as 'other object', occupying a position on in-memory tree, but
skipped on walk iterators. File ownership is ignored.

Optionally, file metadata (file size, mtime, and ctime) is also read and
recorded. In this case, files are also indexed by size.

Commands for manipulating file paths can be executed (moving, renaming,
linking/unlinking) and optionally written back to the disk tree. All
commands are reversible, except those that delete a file's only path.
"""

from __future__ import print_function
import os
import lnsync_pkg.printutils as pr
from lnsync_pkg.fileid import make_id_computer
from lnsync_pkg.glob_matcher import GlobMatcher




class TreeItem(object):
    """Abstract base class for all FileTree items."""
    def is_dir(self):
        return False
    def is_file(self):
        return False

class FileItem(TreeItem):
    __slots__ = "file_id", "file_metadata", "relpaths"
    def __init__(self, file_id, metadata):
        self.file_id = file_id
        self.file_metadata = metadata
        self.relpaths = []
    def is_file(self):
        return True

class DirItem(TreeItem):
    __slots__ = "dir_id", "parent", "entries", "relpath", "scanned"
    def __init__(self, dir_id):
        self.dir_id = dir_id
        self.parent = None # A DirItem.
        self.entries = {} # basename->TreObj.
        self.relpath = None # Cache.
        self.scanned = False
    def was_scanned(self):
        """Return True if dir was scanned."""
        return self.scanned
    def mark_scanned(self):
        """Mark dir as scanned."""
        self.scanned = True
    def add_entry(self, basename, obj):
        assert not basename in self.entries, \
            "add_entry: %s already in dir" %  (basename,)
        self.entries[basename] = obj
        if obj.is_dir():
            obj.parent = self
    def rm_entry(self, basename):
        assert basename in self.entries, \
            "rm_entry: %s not in dir" % (basename,)
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
        assert self.scanned, "cannot iterate unscanned dir"
        for obj in self.entries.itervalues():
            if obj.is_dir():
                yield obj
    def get_relpath(self):
        """Return dir relpath, no trailing os.sep."""
        if self.relpath is None: # Cache.
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

class OtherItem(TreeItem):
    pass

class Metadata(object):
    """File metadata: size, mtime, and ctime.
    Metadata are __equal__ if size and mtime are."""
    __slots__ = "size", "mtime", "ctime"
    __hash__ = None # Since we redefine __eq__, declare not hashable.
    def __init__(self, size, mtime, ctime):
        self.size = size
        self.mtime = mtime
        self.ctime = ctime
    def __eq__(self, other):
        return self.size == other.size and self.mtime == other.mtime
    def __str__(self):
        return "md[%d;%d;%d]" % (self.size, self.mtime, self.ctime)

class FileTree(object):
    """A file tree that can read from disk and write back changes.

    Support files with multiple paths (hard links) and persistent serial number
    id (inode), dirs, and explicit 'other'.
    Files are accessible by file id.
    Optionally, associate metadata (size, ctime, mtime) to files. In this case,
    files are accessible by size.
    Files can be excluded by size limit. File paths and dirs can be excluded by
    glob pattern. Exclude patterns may be set so long as the root dir hasn't
    been scanned yet.
    """
    def __init__(self, **kwargs):
        """Create a root dir object, marked unscanned.
        Arguments:
            - root_path: path disk file tree.
            - exclude_pattern: None or a list of glob patterns for
                relative paths to ignore when reading from disk.
            - use_metadata: if True read metadata index files by size
            - maxsize: ignore files larger than this, is positive and not None
            - skipempty: ignore zero-length files
            - writeback: if True, path manipulation methods update the disk.
            - file_type, dir_type: classes to instantiate.
        """
        self.root_path = os.path.realpath(kwargs.pop("root_path"))
        self.writeback = kwargs.pop("writeback", True)
        self._file_type = kwargs.pop("file_type", FileItem)
        self._dir_type = kwargs.pop("dir_type", DirItem)
        self._use_metadata = kwargs.pop("use_metadata", False)
        if self._use_metadata:
            self._size_to_files = {} # Available if tree has been fully scanned.
            self._size_to_files_ready = False
            maxsize = kwargs.pop("maxsize", -1)
            if maxsize < 0:
                self._maxsize = None
            else:
                self._maxsize = maxsize
            skipempty = kwargs.pop("skipempty", False)
            self._skipempty = skipempty
        self._id_to_file = {}    # May be filled on-demand.
        self._id_computer = make_id_computer(self.root_path)
        self._next_free_dir_id = 1
        self.rootdir_obj = self._new_dir_obj(0)
        self._exclude_patterns = []
        self._exclude_matchers = {}
        self.add_exclude_patterns(kwargs.pop("exclude_patterns", []))

    def add_exclude_patterns(self, patterns):
        self._exclude_patterns += patterns
        if patterns:
            assert not self.rootdir_obj.was_scanned()
            root_matcher = GlobMatcher(self._exclude_patterns)
            self._exclude_matchers = {self.rootdir_obj: root_matcher}

    def printable_path(self, rel_path, pprint=str):
        """Return a pretty-printed full path from a tree relative path."""
        return pprint(os.path.join(self.root_path, rel_path))

    def size_to_files(self, size=None):
        """Return a list of file objects or, if size is None, the internal
        size->files dict. Trigger a full-tree scan, if needed.
        """
        assert self._use_metadata, "size_to_files without metadata."
        if not self._size_to_files_ready:
            self.scan_subtree()
        if size is None:
            return self._size_to_files
        elif size in self._size_to_files:
            return self._size_to_files[size]
        else:
            return []

    def get_all_sizes(self):
        """Return list of all file sizes in the tree."""
        sz__to_files_map = self.size_to_files()
        return sz__to_files_map.keys()

    def id_to_file(self, fid):
        assert fid in self._id_to_file, "id_to_file: unknown fid %d." % fid
        return self._id_to_file[fid]

    def rel_to_abs(self, rel_path):
        """Relative to absolute path."""
        return os.path.join(self.root_path, rel_path)

    def abs_to_rel(self, abs_path):
        """Absolute to relative path."""
        return os.path.relpath(abs_path, self.root_path)

    def _new_dir_obj(self, dir_id=None):
        """Return a new dir object, parent and basename yet undetermined.
        If dir_id is None, it is autoset."""
        if dir_id is None:
            dir_id = self._next_free_dir_id
            self._next_free_dir_id += 1
        dir_obj = self._dir_type(dir_id)
        return dir_obj

    def _new_file_obj(self, obj_id, rawmetadata):
        """Return a new file object. obj_is the file id, rawmetadata is a stat
        record here."""
        stat_data = rawmetadata
        if self._use_metadata:
            metadata = Metadata(stat_data.st_size,
                                int(stat_data.st_mtime),
                                int(stat_data.st_ctime))
        else:
            metadata = None
        file_obj = self._file_type(obj_id, metadata)
        return file_obj

    def scan_dir(self, dir_obj):
        """Scan a directory from disk, if it hasn't been scanned before."""
        assert isinstance(dir_obj, DirItem) #is not None, "scan_dir: no dir_obj"
        if dir_obj.was_scanned():
            return
        with pr.ProgressPrefix(
            "scanning:" + self.printable_path(dir_obj.get_relpath())
            ):
            dir_exclude_matcher = self._exclude_matchers.get(dir_obj)
                # dir_exclude_matcher is None or an entry.
            for (basename, obj_id, obj_type, raw_metadata) in \
                    self._gen_dir_entries_from_source(dir_obj,
                                                      dir_exclude_matcher):
                try:
                    basename.decode('utf-8')
                except Exception:
                    msg = "not a valid utf-8 filename: '%s'. proceeding."
                    msg = msg % (os.path.join(dir_obj.get_relpath(), basename),)
                    pr.warning(msg)
                if obj_type == FileItem:
                    self._scan_dir_process_file(
                        dir_obj, obj_id, basename, raw_metadata)
                elif obj_type == DirItem:
                    self._scan_dir_process_dir(
                        dir_obj, obj_id, dir_exclude_matcher, basename)
                else:
                    dir_obj.add_entry(basename, OtherItem())
            if dir_obj in self._exclude_matchers:
                del self._exclude_matchers[dir_obj]
            dir_obj.mark_scanned()

    def _scan_dir_process_file(self, parent_obj, obj_id, basename, raw_metadata):
        if obj_id in self._id_to_file:
            file_obj = self._id_to_file[obj_id]
        else:
            file_obj = self._new_file_obj(obj_id, raw_metadata)
            if self._use_metadata:
                if (self._skipempty and file_obj.file_metadata.size == 0):
                    obj_abspath = \
                        self.rel_to_abs(os.path.join(
                            parent_obj.get_relpath(), basename))
                    pr.warning("ignored empty file '%s'" % obj_abspath)
                    return
                elif (self._maxsize is not None and \
                        file_obj.file_metadata.size > self._maxsize):
                    obj_abspath = \
                        self.rel_to_abs(os.path.join(
                            parent_obj.get_relpath(), basename))
                    pr.warning("ignored large file '%s'" % obj_abspath)
                    return
        self._add_path(file_obj, parent_obj, basename)

    def _scan_dir_process_dir(
            self, parent_obj, obj_id, parent_excluder, basename):
        self._next_free_dir_id = max(self._next_free_dir_id, obj_id + 1)
        subdir_obj = self._new_dir_obj(obj_id)
        parent_obj.add_entry(basename, subdir_obj)
        if parent_excluder:
            subdir_exclude_matcher = parent_excluder.to_subdir(basename)
            if subdir_exclude_matcher:
                self._exclude_matchers[subdir_obj] = subdir_exclude_matcher

    def scan_subtree(self, start_dir=None):
        """Recursively scan subtree rooted at start_dir."""
        if start_dir is None:
            start_dir = self.rootdir_obj
        self.scan_dir(start_dir)
        for obj in start_dir.entries.values():
            if obj.is_dir() and not obj.was_scanned():
                self.scan_dir(obj)
                self.scan_subtree(obj)
        if start_dir == self.rootdir_obj:
            self._size_to_files_ready = True

    def _gen_dir_entries_from_source(self, dir_obj, exclude_matcher=None):
        """Iterate over the items in a disk file tree.
        Yield tuples (basename, obj_id, obj_type, rawmetadata), where:
        - rawmetadata is some data that may passed on to _new_file_obj.__init__
        - obj_type is one of DirItem, FileItem or OtherItem.
        """
        dir_relpath = dir_obj.get_relpath()
        dir_abspath = self.rel_to_abs(dir_relpath)
        for obj_bname in os.listdir(dir_abspath):
            obj_abspath = os.path.join(dir_abspath, obj_bname)
            if os.path.islink(obj_abspath): # This must be tested for first.
                pr.warning("ignored symlink %s" % obj_abspath)
                yield (obj_bname, None, OtherItem, None)
            elif os.path.isfile(obj_abspath):
                if exclude_matcher \
                        and exclude_matcher.match_file_bname(obj_bname):
                    pr.warning("excluded file %s" % obj_abspath)
                    yield (obj_bname, None, OtherItem, None)
                elif not os.access(obj_abspath, os.R_OK):
                    pr.warning("ignored no-read-access file %s" % obj_abspath)
                    yield (obj_bname, None, OtherItem, None)
                else:
                    obj_relpath = os.path.join(dir_relpath, obj_bname)
                    pr.progress("%s" % obj_relpath)
                    stat_data = os.stat(obj_abspath)
                    fid = self._id_computer.get_id(obj_relpath, stat_data)
                    yield (obj_bname, fid, FileItem, stat_data)
            elif os.path.isdir(obj_abspath):
                if exclude_matcher \
                        and exclude_matcher.match_dir_bname(obj_bname):
                    pr.warning("excluded dir %s" % obj_abspath)
                    yield (obj_bname, None, OtherItem, None)
                elif not os.access(obj_abspath, os.R_OK + os.X_OK):
                    pr.warning("ignored no-rx-access dir %s" % obj_abspath)
                    yield (obj_bname, None, OtherItem, None)
                else:
                    dir_id = self._next_free_dir_id
                    self._next_free_dir_id += 1
                    yield (obj_bname, dir_id, DirItem, None)
            else:
                pr.warning("ignored special file %s" % obj_abspath)
                yield (obj_bname, None, OtherItem, None)

    def _add_path(self, file_obj, dir_obj, fbasename):
        """Add a new path to a file object:  fbasename at dir_obj.
        If this is the first path, the file is registered into
        the tree indices.
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
        """Remove a path from an existing file: fbasename at dir.
        dir must have already been scanned.
        If the final path of a file is removed, the file is removed
        from tree indices."""
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
        paths = list(file_obj.relpaths)
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
        """Generate (obj, basename) for each file and dir entry of
        at a directory given by relative path, which is scanned if needed.
        Skip 'other' entries."""
        assert isinstance(subdir_path, str)
        assert self.rootdir_obj is not None, "walk_dir_contents: no rootdir_obj"
        subdir = self.follow_path(subdir_path)
        assert subdir is not None and subdir.is_dir()
        self.scan_dir(subdir)
        for basename, obj in subdir.entries.iteritems():
            if obj.is_file() or (dirs and obj.is_dir()):
                yield obj, basename

    def walk_paths(self, startdir_path="", recurse=True, dirs=False, files=True, topdown=True):
        """Generate (obj, parent_obj, relpath) for distinct file and dir
        relpaths.
        Skip startdir itself, skip other objects.
         - dirs: include dirs other than startdir.
         - files: include files.
         - recurse: walk the full tree, else just subdir immediate contents.
         - topdown: breadth-first, else depth-first
        """
        assert self.rootdir_obj is not None, "walk_paths: no rootdir_obj."
        startdir_obj = self.follow_path(startdir_path)
        assert startdir_obj is not None \
            and startdir_obj.is_dir(), "walk_paths: dobj not a dir"
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
            while stack:
                    # stack[-2:]=[dirs, unprocessed child dirs of dirs[-1]].
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

    def add_path_writeback(self, file_obj, relpath):
        """Add a new path to an existing file, creating intermediate dirs if
        needed."""
        tr_obj = self._create_dir_if_needed_writeback(os.path.dirname(relpath))
        self._add_path(file_obj, tr_obj, os.path.basename(relpath))
        if self.writeback:
            assert file_obj is not None, "add_path_writeback: no file_obj."
            assert file_obj.relpaths, "add_path_writeback: some path must exist."
            os.link(self.rel_to_abs(file_obj.relpaths[0]),
                    self.rel_to_abs(relpath))

    def rm_path_writeback(self, file_obj, relpath):
        if self.writeback:
            os.unlink(self.rel_to_abs(relpath))
        tr_obj = self.follow_path(os.path.dirname(relpath))
        assert tr_obj is not None and tr_obj.is_dir(), \
            "rm_path_writeback: expected a dir at '%s'." \
                % (os.path.dirname(relpath),)
        self._rm_path(file_obj, tr_obj, os.path.basename(relpath))

    def mv_path_writeback(self, file_obj, fn_from, fn_to):
        """Rename one of the file's paths."""
        # Cannot be done by adding/removing links
        # on filesystems not supporting hardlinks.
        d_from = self.follow_path(os.path.dirname(fn_from))
        assert d_from is not None and d_from.is_dir(), \
            "mv_path_writeback: expected a dir at '%s'." \
                % (os.path.dirname(fn_from),)
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

    def _create_dir_if_needed_writeback(self, dir_relpath):
        """Return dir obj corresponding to dir_relpath, creating all needed
        directories. May raise RuntimeError or OSError."""
        tr_obj = self.follow_path(dir_relpath)
        if tr_obj is None:
            supdname = os.path.dirname(dir_relpath)
            dbasename = os.path.basename(dir_relpath)
            supd = self._create_dir_if_needed_writeback(supdname)
            newd = self._new_dir_obj()
            newd.mark_scanned()
            supd.add_entry(dbasename, newd)
            if self.writeback:
                os.mkdir(self.rel_to_abs(dir_relpath))
            return newd
        elif tr_obj.is_dir():
            return tr_obj
        else:
            raise RuntimeError("cannot create dir at '%s'." % (dir_relpath,))

    def exec_cmd(self, cmd):
        """Execute a (cmd, arg1, arg2).
        cms is one of "mv" ln" "rm"
        arg1 and arg2 are relative paths."""
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

    def exec_cmds(self, cmds):
        for command in cmds:
            self.exec_cmd(command)

    def exec_cmd_reverse(self, cmd):
        """Revert a command (cmd, arg1, arg2)."""
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
        """Revert a sequence of commands."""
        for cmd in reversed(cmds):
            self.exec_cmd_reverse(cmd)

    def __str__(self):
        return "%s(%s)" % (object.__str__(self), self.root_path)
