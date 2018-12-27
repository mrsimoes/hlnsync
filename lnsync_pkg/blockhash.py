#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""Compute file hash, reading a fixed-length block at time, using pyhashxx.
"""

from __future__ import print_function
import pyhashxx

BLOCK_SIZE = 2**20 # A 1MB block read at at time.

def hash_data(data):
    """Return the xxhash of data."""
    return pyhashxx.hashxx(data)

def hash_file(fpath):
    """Return the xxhash for file at fpath."""
    with open(fpath, "rb") as infile:
        hasher = pyhashxx.Hashxx() # Seed is optional.
        while True:
            datab = infile.read(BLOCK_SIZE) # Yields here to other threads.
            if datab == "":
                break
            hasher.update(datab)
    return hasher.digest()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("usage: blockhash <filepath>")
    print(hash_file(sys.argv[1]))
