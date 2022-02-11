#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Return a unique file serial value that persists across successive mounts.
This number is the same for any path referring to the same underlying file
(i.e. hard links).

On a POSIX-compliant file system, this is the inode.
(https://en.wikipedia.org/wiki/Inode#POSIX_inode_description)

On FAT/VFAT: use hash of dirname + int(ctime) + small integer to ensure
uniqueness.
"""

import sys
import os
import abc

from psutil import disk_partitions

from lnsync_pkg.hasher_functions import FileHasherXXHASH64
from lnsync_pkg.miscutils import MIN_INT64, MAX_INT64

FSE = sys.getfilesystemencoding() # Always UTF8 on Linux.

def get_fs_type(path):
    """
    Return file_system for a path.
    """
    path = os.path.realpath(path)
    mntpoint_to_fstype = {}
    for part in disk_partitions():
        mntpoint_to_fstype[part.mountpoint] = part.fstype
    while True:
        if path in mntpoint_to_fstype:
            return mntpoint_to_fstype[path]
        if path == os.sep:
            return None
        path = os.path.dirname(path)

SYSTEMS_W_INODE = ('ext2', 'ext3', 'ext4', 'ecryptfs',
                   'btrfs', 'ntfs', 'fuse.encfs', 'fuseblk')
SYSTEMS_W_INODE += tuple(s.swapcase() for s in SYSTEMS_W_INODE)
SYSTEMS_WO_INODE = ('vfat', 'fat', 'FAT')

def make_id_computer(topdir_path):
    """
    Return an instance of the appropriate IDComputer class.
    """
    file_sys = get_fs_type(topdir_path)
    if file_sys in SYSTEMS_W_INODE:
        return InodeIDComputer(topdir_path, file_sys)
    elif file_sys in SYSTEMS_WO_INODE:
        return HashPathIDComputer(topdir_path, file_sys)
    else:
        raise EnvironmentError(
            "IDComputer: not implemented for file system %s." % file_sys)

class IDComputer:
    """
    Compute persistent, unique file serial numbers for files in a
    tree rooted at a given path.
    The returned value is a signed 64-bit integer.
    """
    def __init__(self, topdir_path, file_sys):
        "Init with rootdir on which relative file paths are based."
        self._topdir_path = topdir_path
        self.file_sys = file_sys
        self.subdir_invariant = True

    @abc.abstractmethod
    def get_id(self, rel_path, stat_data=None):
        """
        Return serial number for file at relative path from the root.
        """

class InodeIDComputer(IDComputer):
    """
    Return inode as file serial number.
    """
    def get_id(self, rel_path, stat_data=None):
        if stat_data is None:
            stat_data = os.stat(os.path.join(self._topdir_path, rel_path))
        return stat_data.st_ino

class HashPathIDComputer(IDComputer):
    """
    Return hash(path)+size(file)+smallint as file serial number.
    """
    def __init__(self, topdir_path, file_sys):
        self._hasher = FileHasherXXHASH64()
        self._hash_plus_size_uniq = {}
        super().__init__(topdir_path, file_sys)
        self.subdir_invariant = False

    def get_id(self, rel_path, stat_data=None):
        if stat_data is None:
            stat_data = os.stat(os.path.join(self._topdir_path, rel_path))
        file_id = 0
        for path_component in rel_path.split(os.sep)[:-1]:
            path_component = path_component.encode(FSE, "surrogateescape")
            file_id += self._hasher.hash_datum(path_component)
        file_id += stat_data.st_size
        while file_id in self._hash_plus_size_uniq:
            if self._hash_plus_size_uniq[file_id] == rel_path:
                return file_id
            else:
                file_id += 1
        # Make sure we return a (signed) int64
        if file_id > MAX_INT64:
            file_id %= (MAX_INT64 + 1)
        if file_id < MIN_INT64:
            file_id = - (-file_id % (-MIN_INT64+1))
        self._hash_plus_size_uniq[file_id] = rel_path
        return file_id
