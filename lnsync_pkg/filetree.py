#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Manage a file tree in a mounted filesystem, with support for hard links.

A file tree item is either a file or a directory or catch-all "other" item.

Paths relative to the tree root are used throughout. Because file hard links are
supported, files are distinct from file paths. Each file has one or more file
paths.

File are assigned a file id (a persistent serial number, e.g. inode) and are
accessible by file id. Directories are also assigned ids. The root has id zero.

An in-memory representation is built by scanning the disk file tree. Scanning is
done a need-to basis and scanned directories are marked. While scanning, items
of the disk tree may be ignored using glob patters.

Only readable files and read/exec directories are read as files and directories.
Other files, other dirs, all symlinks and special files are read as 'other
item', occupying a position on in-memory tree, but skipped on walk iterators.
Excluded objects are also explicitly read as 'excluded item'. File ownership is
ignored.


Optionally, file metadata (file size, mtime, and ctime) is also read and
recorded. In this case, files are also indexed by size.

Commands for manipulating file paths can be executed (moving, renaming,
linking/unlinking) and optionally written back to the disk tree. All commands
are reversible, except those that delete a file's only path.
"""

# pylint: disable=too-many-public-methods, too-many-instance-attributes

import os

import lnsync_pkg.printutils as pr
from lnsync_pkg.fileid import make_id_computer
from lnsync_pkg.glob_matcher import GlobMatcher

class TreeError(Exception):
    def __init__(self, msg, tree=None, file_obj=None):
        super().__init__(msg)
        self.tree = tree
        self.file_obj = file_obj

    def file_str(self):
        fid = self.file_obj.file_id
        path_digest = self.tree.printable_file_path_digest(self.file_obj)
        return f"<id:{fid}>, paths: {path_digest}>"

    def __str__(self):
        err_str = super().__str__()
        if self.file_obj:
            err_str += ": file " + self.file_str()
        return err_str

class TreeItem:
    """
    Abstract base class for all FileTree items.
    """

    @staticmethod
    def is_dir():
        return False

    @staticmethod
    def is_file():
        return False

    @staticmethod
    def is_excluded():
        return False

class FileItem(TreeItem):
    __slots__ = "file_id", "file_metadata", "relpaths"

    def __init__(self, file_id, metadata):
        self.file_id = file_id
        self.file_metadata = metadata
        self.relpaths = []

    @staticmethod
    def is_file():
        return True

class DirItem(TreeItem):
    __slots__ = "dir_id", "parent", "entries", "relpath", "scanned"

    def __init__(self, dir_id):
        self.dir_id = dir_id
        self.parent = None # A DirItem.
        self.entries = {} # basename->TreeObj.
        self.relpath = None # Cache.
        self._scanned = False

    def was_scanned(self):
        """
        Test if dir was scanned.
        """
        return self._scanned

    def mark_scanned(self):
        """
        Mark dir as scanned.
        """
        self._scanned = True

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
        assert self._scanned, \
            "cannot iterate unscanned dir"
        for obj in self.entries.values():
            if obj.is_dir():
                yield obj

    def get_relpath(self):
        """
        Return dir relpath, no trailing os.sep.
        """
        if self.relpath is None: # Cache.
            curr_dir = self
            path = None
            while curr_dir.parent is not None:
                for entryname, obj in curr_dir.parent.entries.items():
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

class ExcludedItem(TreeItem):
    def is_excluded(self):
        return True

class Metadata:
    """
    File metadata: size, mtime, and ctime.
    Metadata are __equal__ if size and mtime are.
    """
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

class FileTree:
    """
    A file tree that can read from disk and write back changes.

    Support files with multiple paths (hard links) and persistent serial number
    id (inode), dirs, and explicit 'other'.
    Files are accessible by file id.
    Optionally, associate metadata (size, ctime, mtime) to files. In this case,
    files are accessible by size.
    Files can be excluded by size limit. File paths and dirs can be excluded by
    glob pattern. Exclude patterns may be set so long as the root dir hasn't
    been scanned yet.
    """

    @classmethod
    def scan_trees_async(cls, trees):
        for tree in trees:
            tree.scan_subtree()

    def __init__(self, **kwargs):
        """
        Create a root dir object, marked unscanned.
        Arguments:
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
        self.topdir_path = kwargs.pop("topdir_path")
        if self.topdir_path is not None:
            self.topdir_path = os.path.realpath(self.topdir_path)
            self._id_computer = make_id_computer(self.topdir_path)
        self.writeback = kwargs.pop("writeback", True)
        self._file_type = kwargs.pop("file_type", FileItem)
        self._dir_type = kwargs.pop("dir_type", DirItem)
        self._use_metadata = kwargs.pop("use_metadata", False)
        if self._use_metadata:
            self._size_to_files = {} # Available if tree has been fully scanned.
            self._size_to_files_ready = False
            self._unscanned_dir_count = 1
            maxsize = kwargs.pop("maxsize", -1)
            if maxsize < 0:
                self._maxsize = None
            else:
                self._maxsize = maxsize
            skipempty = kwargs.pop("skipempty", False)
            self._skipempty = skipempty
        self._id_to_file = {}    # May be filled on-demand.
        self._next_free_dir_id = 1
        self.rootdir_obj = self._new_dir_obj(0)
        self._glob_patterns = []
        # dir-> matcher, must be set before dir is scanned.
        self._glob_matchers = {}
        self.add_glob_patterns(kwargs.pop("exclude_patterns", []))

    def add_glob_patterns(self, patterns, before=True):
        if patterns:
            if before:
                self._glob_patterns = patterns + self._glob_patterns
            else:
                self._glob_patterns += patterns
            assert not self.rootdir_obj.was_scanned(), \
                "add_glob_patterns: cannot add patterns after scanning root"
            root_matcher = GlobMatcher(self._glob_patterns)
            self._glob_matchers = {self.rootdir_obj: root_matcher}

    def printable_path(self, rel_path=None, pprint=str):
        """
        Return a pretty-printed full path from a tree relative path.
        If rel_path is None, default to root directory.
        """
        if rel_path is None:
            rel_path = ""
        return pprint(os.path.join(self.topdir_path, rel_path))

    def printable_file_path_digest(self, file_obj=None, pprint=str):
        """
        Return the unique path, pretty-printed and quoted, "'{path1}'"
        or "['{path1}', ...]" if the path has more than one hard link.
        """
        first_path_pp = self.printable_path(
            file_obj.relpaths[0],
            pprint=pprint)
        if len(file_obj.relpaths) == 1:
            return first_path_pp
        else:
            return f"[{first_path_pp}, ...]"

    def walk_files(self, topdir=None):
        """
        Yield all file objects, scanning as needed.
        Files, not paths.
        """
        if topdir is None:
            topdir = self.rootdir_obj
        yielded_files = set()
        dirs_to_scan = [topdir]
        while dirs_to_scan:
            next_dir = dirs_to_scan.pop()
            self.scan_dir(next_dir)
            for _basename, obj in next_dir.entries.items():
                if obj.is_file():
                    if obj not in yielded_files:
                        yielded_files.add(obj)
                        yield obj
                elif obj.is_dir():
                    dirs_to_scan.append(obj)

    def size_to_files_gen(self, size=None):
        """
        Return a generator of file objects of a given size.
        If size is None, yield all files.
        """
        for fobj in self.walk_files():
            if size is None or fobj.file_metadata.size == size:
                yield fobj

    def size_to_files(self, size=None):
        """
        Return a list of file objects of a given size.
        Trigger a full-tree scan, if needed.
        If size is None, return a list of all files. TODO needed?
        """
        assert self._use_metadata, \
            "size_to_files without metadata."
        if not self._size_to_files_ready:
            self.scan_subtree()
        if size is None:
            import ipdb; ipdb.set_trace() # TODO ensure this has no clients.
            return self._size_to_files.values()
        elif size in self._size_to_files:
            return self._size_to_files[size]
        else:
            return []

    def get_file_count(self):
        """
        Return total number of files, after scanning the full tree.
        """
        self.scan_subtree()
        return sum(len(szfiles) \
                for (sz, szfiles) in self._size_to_files.items())

    def get_all_sizes(self):
        """
        Return list of all file sizes in the tree.
        """
        if not self._size_to_files_ready:
            self.scan_subtree()
        return list(self._size_to_files.keys())

    def get_all_file_ids(self):
        """
        Return a dictionary view object with all file ids.
        (Used by outside utils.)
        """
        self.scan_subtree()
        return self._size_to_files.keys()

    def get_all_dirs(self):
        """
        Return a set with all dir objects by scanning the full tree.
        (Used by outside utils.)
        """
        self.scan_subtree()
        res = [self.rootdir_obj]
        for obj, _pobj, _rp in self.walk_paths(dirs=True):
            if obj.is_dir():
                res.append(obj)
        return res

    def id_to_file(self, fid):
        """
        Return None if no such file.
        """
        if not fid in self._id_to_file:
            return None
        else:
            return self._id_to_file[fid]

    def rel_to_abs(self, rel_path):
        """
        Relative to absolute path.
        """
        return os.path.join(self.topdir_path, rel_path)

    def abs_to_rel(self, abs_path):
        """
        Absolute to relative path.
        """
        return os.path.relpath(abs_path, self.topdir_path)

    def _new_dir_obj(self, dir_id=None):
        """
        Return a new dir object, parent and basename yet undetermined.
        If dir_id is None, it is autoset.
        """
        if dir_id is None:
            dir_id = self._next_free_dir_id
            self._next_free_dir_id += 1
        dir_obj = self._dir_type(dir_id)
        return dir_obj

    def _new_file_obj(self, obj_id, rawmetadata):
        """
        Return a new file object. obj_is the file id, rawmetadata is a stat
        record here.
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

    def scan_dir(self, dir_obj, clear_on_exit=True):
        """
        Scan a directory from disk, if it hasn't been scanned before.
        """
        assert isinstance(dir_obj, DirItem), \
            "scan_dir: not a DirItem"
        if dir_obj.was_scanned():
            return
        with pr.ProgressPrefix(
                "scanning:" + self.printable_path(dir_obj.get_relpath()),
                clear_on_exit=clear_on_exit,
            ):
            dir_glob_matcher = self._glob_matchers.get(dir_obj)
                # dir_glob_matcher is None or an entry.
            for (basename, obj_id, obj_type, raw_metadata) in \
                    self._gen_dir_entries_from_source(
                            dir_obj,
                            dir_glob_matcher):
                if obj_type == FileItem:
                    self._scan_dir_process_file(
                        dir_obj, obj_id, basename, raw_metadata)
                elif obj_type == DirItem:
                    self._scan_dir_process_dir(
                        dir_obj, obj_id, dir_glob_matcher, basename)
                else:
                    dir_obj.add_entry(basename, obj_type())
            if dir_obj in self._glob_matchers:
                del self._glob_matchers[dir_obj]
            dir_obj.mark_scanned()
            if self._use_metadata:
                self._unscanned_dir_count -= 1
                if not self._unscanned_dir_count:
                    self._size_to_files_ready = True

    def _scan_dir_process_file(self, parent_obj, obj_id,
                               basename, raw_metadata):
        if obj_id in self._id_to_file:
            file_obj = self._id_to_file[obj_id]
        else:
            file_obj = self._new_file_obj(obj_id, raw_metadata)
            if self._use_metadata:
                if (self._skipempty and file_obj.file_metadata.size == 0):
                    obj_path = \
                        self.printable_path(os.path.join(
                            parent_obj.get_relpath(), basename))
                    pr.debug("ignored empty file %s", obj_path)
                    return
                elif (self._maxsize is not None and \
                        file_obj.file_metadata.size > self._maxsize):
                    obj_path = \
                        self.printable_path(os.path.join(
                            parent_obj.get_relpath(), basename))
                    pr.debug("ignored large file %s", obj_path)
                    return
        self._add_path(file_obj, parent_obj, basename)

    def _scan_dir_process_dir(
            self, parent_obj, obj_id, parent_glob, basename):
        self._next_free_dir_id = max(self._next_free_dir_id, obj_id + 1)
        subdir_obj = self._new_dir_obj(obj_id)
        parent_obj.add_entry(basename, subdir_obj)
        if self._use_metadata:
            self._unscanned_dir_count += 1
        if parent_glob:
            subdir_glob_matcher = parent_glob.to_subdir(basename)
            if subdir_glob_matcher:
                self._glob_matchers[subdir_obj] = subdir_glob_matcher

    def scan_subtree(self, start_dir=None, clear_on_exit=True):
        """
        Recursively scan subtree rooted at start_dir.
        """
        if start_dir is None:
            start_dir = self.rootdir_obj
        self.scan_dir(start_dir, clear_on_exit=False)
        for obj in start_dir.entries.values():
            if obj.is_dir() and not obj.was_scanned():
                self.scan_dir(obj, clear_on_exit=False)
                self.scan_subtree(obj, clear_on_exit=False)
        if self._use_metadata and \
                (start_dir == self.rootdir_obj or self._unscanned_dir_count == 0):
            self._size_to_files_ready = True
        if clear_on_exit:
            pr.progress("")

    def _gen_dir_entries_from_source(self, dir_obj, glob_matcher=None):
        """
        Iterate over the items in a disk file tree.
        Yield tuples (basename, obj_id, obj_type, rawmetadata), where:
        - basename is str (Python2) or binary string (Python3)
        - rawmetadata is some data that may passed on to _new_file_obj.__init__
        - obj_type is one of DirItem, FileItem, OtherItem or ExcludedItem.
        """
        dir_relpath = dir_obj.get_relpath()
        dir_abspath = self.rel_to_abs(dir_relpath)
        for obj_bname in os.listdir(dir_abspath):
            obj_abspath = os.path.join(dir_abspath, obj_bname)
            if os.path.islink(obj_abspath): # This must be tested for first.
                if glob_matcher \
                        and glob_matcher.exclude_file_bname(obj_bname):
                    pr.debug("excluded symlink %s", obj_abspath)
                    yield (obj_bname, None, ExcludedItem, None)
                else:
                    pr.debug("ignored symlink %s", obj_abspath)
                    yield (obj_bname, None, OtherItem, None)
            elif os.path.isfile(obj_abspath):
                if glob_matcher \
                        and glob_matcher.exclude_file_bname(obj_bname):
                    pr.debug("excluded file %s", obj_abspath)
                    yield (obj_bname, None, ExcludedItem, None)
                elif not os.access(obj_abspath, os.R_OK):
                    pr.debug("ignored no-read-access file %s", obj_abspath)
                    yield (obj_bname, None, OtherItem, None)
                else:
                    obj_relpath = os.path.join(dir_relpath, obj_bname)
                    pr.progress(obj_relpath)
                    stat_data = os.stat(obj_abspath)
                    fid = self._id_computer.get_id(obj_relpath, stat_data)
                    yield (obj_bname, fid, FileItem, stat_data)
            elif os.path.isdir(obj_abspath):
                if glob_matcher \
                        and glob_matcher.exclude_dir_bname(obj_bname):
                    pr.debug("excluded dir %s", obj_abspath)
                    yield (obj_bname, None, ExcludedItem, None)
                elif not os.access(obj_abspath, os.R_OK + os.X_OK):
                    pr.debug("ignored no-rx-access dir %s", obj_abspath)
                    yield (obj_bname, None, OtherItem, None)
                else:
                    dir_id = self._next_free_dir_id
                    self._next_free_dir_id += 1
                    yield (obj_bname, dir_id, DirItem, None)
            else:
                if glob_matcher \
                        and glob_matcher.exclude_file_bname(obj_bname):
                    pr.debug("excluded special file %s", obj_abspath)
                    yield (obj_bname, None, ExcludedItem, None)
                else:
                    pr.debug("ignored special file %s", obj_abspath)
                    yield (obj_bname, None, OtherItem, None)

    def _add_path(self, file_obj, dir_obj, fbasename):
        """
        Add a new path to a file object:  fbasename at dir_obj.
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
        """
        Remove a path from an existing file: fbasename at dir.
        dir must have already been scanned.
        If the final path of a file is removed, the file is removed
        from tree indices.
        """
        dir_obj.rm_entry(fbasename)
        relpath = os.path.join(dir_obj.get_relpath(), fbasename)
        assert relpath in file_obj.relpaths, \
            "_rm_path: non-existing relpath."
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
        """
        Remove a file, i.e. remove all paths.
        """
        paths = list(file_obj.relpaths)
        for path in paths:
            tr_obj = self.path_to_obj(os.path.dirname(path))
            self._rm_path(file_obj, tr_obj, os.path.basename(path))

    def path_to_obj(self, relpath):
        """
        Return file or dir or other object by relpath from root, or None.
        Do not follow symlinks.
        """
        assert self.rootdir_obj is not None, \
            "path_to_obj: no rootdir_obj."
        curdir_obj = self.rootdir_obj
        components = relpath.split(os.sep)
        while components and curdir_obj:
            comp = components[0]
            components = components[1:]
            if comp in (".", ""):
                continue
            if curdir_obj.is_dir() and not curdir_obj.was_scanned():
                self.scan_dir(curdir_obj)
            next_obj = curdir_obj.get_entry(comp)
            if next_obj is None or next_obj.is_dir() or not components:
                curdir_obj = next_obj
            else:
                curdir_obj = None
        return curdir_obj

    def walk_dir_contents(self, subdir_path, dirs=False):
        """
        Generate (obj, basename) for each file and dir entry of
        at a directory given by relative path, which is scanned if needed.
        Skip 'other' entries.
        """
        assert self.rootdir_obj is not None, \
            "walk_dir_contents: no rootdir_obj"
        subdir = self.path_to_obj(subdir_path)
        assert subdir is not None and subdir.is_dir(), \
            f"walk_dir_contents: not a dir: {subdir}"
        self.scan_dir(subdir)
        for basename, obj in subdir.entries.items():
            if obj.is_file() or (dirs and obj.is_dir()):
                yield obj, basename

    def walk_paths(self, startdir_path=None, recurse=True,
                   dirs=False, files=True, topdown=True):
        """
        Generate (obj, parent_obj, relpath) for distinct file and dir
        relpaths.
        Skip startdir itself, skip other objects.
         - dirs: include dirs (other than startdir, which is never included).
         - files: include files.
         - recurse: walk the full tree, else just subdir immediate contents.
         - topdown: If False, bottom-up.
        """
        assert self.rootdir_obj is not None, \
            "walk_paths: no rootdir_obj."
        if startdir_path is None:
            startdir_path = ""
        startdir_obj = self.path_to_obj(startdir_path)
        assert startdir_obj is not None \
            and startdir_obj.is_dir(), "walk_paths: dobj not a dir"

        def output_files(dir_obj):
            dir_path = dir_obj.get_relpath()
            for basename, obj in dir_obj.entries.items():
                if (files and obj.is_file()):
                    yield obj, dir_obj, os.path.join(dir_path, basename)

        def output_dir(dir_obj):
            if dirs and dir_obj != startdir_obj:
                dir_path = dir_obj.get_relpath()
                yield dir_obj, dir_obj.parent, dir_path

        def walk_topdown():
            stack = [startdir_obj]
            while stack:
                curdir_obj = stack.pop()
                for k in output_dir(curdir_obj):
                    yield k
                self.scan_dir(curdir_obj)
                for k in output_files(curdir_obj):
                    yield k
                for subd_obj in curdir_obj.iter_subdirs():
                    stack.append(subd_obj)

        def walk_bottomup():
            stack = [[None], [startdir_obj]]
            while stack:
                  # At all times:
                  # stack[-2:]=dirs, stack[-1]=unprocessed children of dirs[-1].
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
                    for k in output_dir(prevdir):
                        yield k
                    for k in output_files(prevdir):
                        yield k

        if not recurse:
            for k in output_files(startdir_obj):
                yield k
            for subd_obj in startdir_obj.iter_subdirs():
                for k in output_dir(subd_obj):
                    yield k
        elif topdown:
            for k in walk_topdown():
                yield k
        else:
            for k in walk_bottomup():
                yield k

    def add_path_writeback(self, file_obj, relpath):
        """
        Add a new path to an existing file, creating intermediate dirs if
        needed.
        """
        tr_obj = self._create_dir_if_needed_writeback(os.path.dirname(relpath))
        self._add_path(file_obj, tr_obj, os.path.basename(relpath))
        if self.writeback:
            assert file_obj is not None, \
                "add_path_writeback: no file_obj."
            assert file_obj.relpaths, \
                "add_path_writeback: no path exists."
            os.link(self.rel_to_abs(file_obj.relpaths[0]),
                    self.rel_to_abs(relpath))

    def rm_path_writeback(self, file_obj, relpath):
        if self.writeback:
            os.unlink(self.rel_to_abs(relpath))
        tr_obj = self.path_to_obj(os.path.dirname(relpath))
        assert tr_obj is not None and tr_obj.is_dir(), \
            "rm_path_writeback: expected a dir at " + \
                os.path.dirname(relpath)
        self._rm_path(file_obj, tr_obj, os.path.basename(relpath))

    def mv_path_writeback(self, file_obj, fn_from, fn_to):
        """
        Rename one of the file's paths.
        """
        # Cannot be done by adding/removing links
        # on filesystems not supporting hard links.
        d_from = self.path_to_obj(os.path.dirname(fn_from))
        assert d_from is not None and d_from.is_dir(), \
            "mv_path_writeback: expected a dir at " + \
                os.path.dirname(fn_from)
        d_to = self._create_dir_if_needed_writeback(os.path.dirname(fn_to))
        self._add_path(file_obj, d_to, os.path.basename(fn_to))
        self._rm_path(file_obj, d_from, os.path.basename(fn_from))
        if self.writeback:
            os.rename(self.rel_to_abs(fn_from), self.rel_to_abs(fn_to))

    def rm_dir_writeback(self, dir_obj):
        """
        Execute a rmdir, write back to source tree if self.writeback is set.
        dir_obj cannot be the root directory.
        If self.writeback, OSError may be thrown if the dir cannot be removed.
        If the dir_obj is non-empty, also throw OSError.
        """
        assert dir_obj.parent, \
            "trying to remove rootdir,"
        relpath = dir_obj.get_relpath()
        if dir_obj.entries:
            raise OSError("trying to remove non-empty dir: %s" % (relpath,))
        if self.writeback:
            os.rmdir(self.rel_to_abs(relpath))
        basename = os.path.basename(relpath)
        dir_obj.parent.rm_entry(basename)

    def _create_dir_if_needed_writeback(self, dir_relpath):
        """
        Return dir obj corresponding to dir_relpath, creating all needed
        directories. May raise TreeError.
        """
        tr_obj = self.path_to_obj(dir_relpath)
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
            raise TreeError("cannot create dir " + dir_relpath)

    def exec_cmd(self, cmd):
        """
        Execute a (cmd, arg1, arg2).
        cms is one of "mv" ln" "rm"
        arg1 and arg2 are relative paths.
        """
        ctype, fn_from, fn_to = cmd
        obj_from = self.path_to_obj(fn_from)
        assert obj_from is not None and obj_from.is_file(), \
            "exec_cmd: expected a file at " + fn_from
        if fn_to is not None:
            obj_to = self.path_to_obj(fn_to)
        else:
            obj_to = None
        if ctype == "mv":
            assert obj_to is None, \
                "exec_cmd: no obj_to."
            self.mv_path_writeback(obj_from, fn_from, fn_to)
        elif ctype == "ln":
            assert obj_to is None, \
                "exec_cmd: no obj_to."
            self.add_path_writeback(obj_from, fn_to,)
        elif ctype == "rm":
            self.rm_path_writeback(obj_from, fn_from)
        else:
            raise TreeError("exec_cmd: unknown command %s" % (cmd,))

    def exec_cmds(self, cmds):
        for command in cmds:
            self.exec_cmd(command)

    def exec_cmd_reverse(self, cmd):
        """
        Revert a command (cmd, arg1, arg2).
        """
        assert len(cmd) == 3, \
            "exec_cmd_reverse: bad cmd: %s" % (cmd,)
        ctype, fn_from, fn_to = cmd
        if ctype == "mv":
            self.exec_cmd(("mv", fn_to, fn_from))
        elif ctype == "ln":
            self.exec_cmd(("rm", fn_to, fn_from)) # Remove link, retain witness.
        elif ctype == "rm":
            witness_obj = self.path_to_obj(fn_to)
            if witness_obj is None or not witness_obj.is_file():
                raise TreeError("exec_cmd_reverse: cannot undo this rm cmd")
            self.exec_cmd(("ln", fn_to, fn_from)) # Recover link from witness.
        else:
            raise TreeError("exec_cmd_reverse: unknown command %s" % (cmd,))

    def exec_cmds_reverse(self, cmds):
        """
        Revert a sequence of commands.
        """
        for cmd in reversed(cmds):
            self.exec_cmd_reverse(cmd)

    def __str__(self):
        return "%s(%s)" % (object.__str__(self), self.topdir_path)
