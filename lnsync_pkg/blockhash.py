#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Compute file hash using pyhashxx, reading a fixed-length block at time.

For very large files (>1 GiB), use a reader/hasher thread combo.
"""

import os
import pyhashxx

from lnsync_pkg.thread_utils import ProducerConsumerThreaded, NoMoreData

ASYNC_SIZE_THRESH = 512 * 2**20 # Files larger than 512 Mib
                                # are processed asynchronously, with threads.

SYNC_BLOCK_SIZE = 4 * 2**20   # 4 MiB blocks at a time for small files.
ASYNC_BLOCK_SIZE = 16 * 2**20  # 16 MiB blocks for large files.

def hash_data(data):
    """Return the xxhash of data."""
    return pyhashxx.hashxx(data)

def hash_file(fpath):
    if os.path.getsize(fpath) >= ASYNC_SIZE_THRESH:
        return hash_file_async(fpath)
    else:
        return hash_file_sync(fpath)

class ReaderHasher(ProducerConsumerThreaded):
    def __init__(self, infile):
        self.hasher = pyhashxx.Hashxx() # Seed is optional.
        self.infile = infile
        self.BLOCK_SIZE = ASYNC_BLOCK_SIZE
        super().__init__()
    def produce(self):
        datum = self.infile.read(self.BLOCK_SIZE)
        if not datum:
            raise NoMoreData
        else:
            return datum
    def consume(self, datum):
        self.hasher.update(datum)
    def run(self):
        super().run()
        return self.hasher.digest()

def hash_file_async(fpath):
    with open(fpath, "rb") as infile:
        readerhasher = ReaderHasher(infile)
        return readerhasher.run()

def hash_file_sync(fpath):
    """
    Return the xxhash for file at fpath.
    """
    BLOCK_SIZE = SYNC_BLOCK_SIZE
    with open(fpath, "rb") as infile:
        hasher = pyhashxx.Hashxx() # Seed is optional.
        while True:
            datab = infile.read(BLOCK_SIZE)
            if not datab:
                break
            hasher.update(datab)
    return hasher.digest()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("usage: blockhash <filepath>")
    print(hash_file(sys.argv[1]))
