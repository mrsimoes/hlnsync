#!/usr/bin/env python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Compute file hash using pyhashxx, reading a fixed-length block at time.

For very large files (>1 GiB), use a reader/hasher thread combo.
"""

import os
import subprocess
import pyhashxx

from lnsync_pkg.thread_utils import ProducerConsumerThreaded, NoMoreData

ASYNC_SIZE_THRESH = 512 * 2**20 # Files larger than 512 Mib
                                # are processed asynchronously, with threads.

SYNC_BLOCK_SIZE = 4 * 2**20   # 4 MiB blocks at a time for small files.
ASYNC_BLOCK_SIZE = 16 * 2**20  # 16 MiB blocks for large files.

def hash_data(data):
    """
    Return the xxhash of data.
    """
    return pyhashxx.hashxx(data)

def hash_file(fpath, filter_exec=None):
    if not filter_exec:
        with open(fpath, "rb") as infile:
            if os.path.getsize(fpath) >= ASYNC_SIZE_THRESH:
                return hash_file_async(infile)
            else:
                return hash_file_sync(infile)
    else:
        try:
            procres = subprocess.run(
                ["/bin/sh", filter_exec, fpath],
                check=True, capture_output=True)
        except subprocess.CalledProcessError as exc:
            msg = "failed hashing %s (%s)" % (fpath, procres.output)
            raise RuntimeError(msg) from exc
        try:
            res = int(procres.stdout)
        except ValueError as exc:
            msg = "invalid output for $(%s %s): %s" % \
                            (filter_exec, fpath, procres.stdout)
            raise Exception(msg) from exc
        return res


class ReaderHasher(ProducerConsumerThreaded):
    """
    Read and hash in parallel threads.
    """
    def __init__(self, infile):
        self.hasher = pyhashxx.Hashxx() # Seed is optional.
        self.infile = infile
        self.block_size = ASYNC_BLOCK_SIZE
        super().__init__()
    def produce(self):
        datum = self.infile.read(self.block_size)
        if not datum:
            raise NoMoreData
        else:
            return datum
    def consume(self, datum):
        self.hasher.update(datum)
    def run(self):
        super().run()
        return self.hasher.digest()

def hash_file_async(infile):
    readerhasher = ReaderHasher(infile)
    return readerhasher.run()

def hash_file_sync(infile):
    """
    Return the xxhash for given open file.
    """
    block_size = SYNC_BLOCK_SIZE
    hasher = pyhashxx.Hashxx() # Seed is optional.
    while True:
        datab = infile.read(block_size)
        if not datab:
            break
        hasher.update(datab)
    return hasher.digest()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("usage: blockhash <filepath> or blockhash -")
    PARAM = sys.argv[1]
    if PARAM == "-":
        RES = hash_file_sync(sys.stdin.buffer)
    else:
        RES = hash_file(PARAM)
    print(RES)
