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
import abc

from psutil import disk_partitions
import lnsync_pkg.blockhash as blockhash

def get_fs_type(path):
    """Return file_system for a path.
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

def make_id_computer(root_path):
    """Return an instance of the appropriate IDComputer class.
    """
    file_sys = get_fs_type(root_path)
    if file_sys in ('ext2', 'ext3', 'ext4', 'ecryptfs', 'btrfs',
                    'ntfs', 'fuse.encfs', 'fuseblk',
                   ):
        return InodeIDComputer(root_path)
    elif file_sys in ('vfat',
                     ):
        return HashPathIDComputer(root_path)
    else:
        raise EnvironmentError(
            "IDComputer: not implemented for file system %s." % file_sys)

class IDComputer(object):
    """Compute persistent, unique file serial numbers for files in a
    tree rooted at a given path.
    """
    def __init__(self, root_path):
        "Init with rootdir on which relative file paths are based."
        self._root_path = root_path

    @abc.abstractmethod
    def get_id(self, rel_path, stat_data=None):
        """Return serial number for file at relative path from the root."""

class InodeIDComputer(IDComputer):
    """Return inode as file serial number.
    """
    def get_id(self, rel_path, stat_data=None):
        if stat_data is None:
            stat_data = os.stat(os.path.join(self._root_path, rel_path))
        return stat_data.st_ino

class HashPathIDComputer(IDComputer):
    """Return hash(path)+size(file)+smallint as file serial number.
    """
    def __init__(self, root_path):
        self._hash_plus_size_uniq = {}
        super(HashPathIDComputer, self).__init__(root_path)
    def get_id(self, rel_path, stat_data=None):
        if stat_data is None:
            stat_data = os.stat(os.path.join(self._root_path, rel_path))
        file_id = blockhash.hash_data(os.path.dirname(rel_path))
        file_id += stat_data.st_size
        while file_id in self._hash_plus_size_uniq:
            if self._hash_plus_size_uniq[file_id] == rel_path:
                return file_id
            else:
                file_id += 1
        self._hash_plus_size_uniq[file_id] = rel_path
        return file_id
