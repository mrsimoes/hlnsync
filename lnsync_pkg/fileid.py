#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Return a unique file serial value that persists across successive mounts.
This number is the same for any path referring to the same underlying file
(i.e. hardlinks).

On a POSIX-compliant file system, this is the inode.
(https://en.wikipedia.org/wiki/Inode#POSIX_inode_description)

On FAT/VFAT: use hash of dirname + int(ctime) + small integer to ensure
uniqueness.
"""

from __future__ import print_function
import os
from psutil import disk_partitions
import lnsync_pkg.blockhash as blockhash

def get_fs_type(path):
    """
    Return file_system for a path.
    """
    path = os.path.realpath(path)
    partition = {}
    for part in disk_partitions():
        partition[part.mountpoint] = part.fstype
    if path in partition:
        return partition[path]
    splitpath = path.split(os.sep)
    for i in range(len(splitpath), 0, -1):
        path = os.sep.join(splitpath[:i]) + os.sep
        if path in partition:
            return partition[path]
        path = os.sep.join(splitpath[:i])
        if path in partition:
            return partition[path]
    return None

class IDComputer(object):
    """Compute inode-like file serial numbers for files under a given root dir.
    """
    def __init__(self, rootdir_path):
        self.rootdir_path = rootdir_path
        self.fs_type = get_fs_type(self.rootdir_path)
        if self.fs_type in \
            ('ext2', 'ext3', 'ext4', 'ecryptfs', 'btrfs', \
            'ntfs', 'fuse.encfs', 'fuseblk'):
            self.get_id = self._compute_inode
        elif self.fs_type in ('vfat',):
            self._hash_plus_size_uniq = {}  # Serial numbers unique on each run.
            self.get_id = self._compute_dirname_hash_plus_size
        else:
            raise EnvironmentError(
                "IDComputer: not implemented for file system %s." % self.fs_type)

#    def get_id(self, rel_path):
#        """Return serial number for file at relative path from the root.
#        """
#        raise NotImplementedError("get_id: not implemented.")

    def _compute_inode(self, rel_path, stat_data=None):
        if stat_data is None:
            stat_data = os.stat(os.path.join(self.rootdir_path, rel_path))
        return stat_data.st_ino

    def _compute_dirname_hash_plus_size(self, rel_path, stat_data):
        file_id = blockhash.hash_data(os.path.dirname(rel_path))
        file_id += stat_data.st_size
        while file_id in self._hash_plus_size_uniq:
            if self._hash_plus_size_uniq[file_id] == rel_path:
                return file_id
            else:
                file_id += 1
        self._hash_plus_size_uniq[file_id] = rel_path
        return file_id
        