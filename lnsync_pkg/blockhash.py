#!/usr/bin/python3

# Copyright (C) 2021 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
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
Compute file hash using either pyhashxx or xxhash, reading a fixed-length block
at time.

Values returned are always converted to *signed* int64.

For very large files (>1 GiB), use a reader/hasher thread combo.

The hashing algorithm is either:
    - xxhash: supports xxhash32 as well as a few other hasher functions.

    - pyhashxx: implements xxhash32, which returns a 32-bit value.
    This was the only choice in earlier versions. It requires compilation
    when installing via pip on Windows.

A class Hasher is provided, which can be used without creating instances,
since settings and methods are global.

To prevent importing unneeded libraries, while allowing for easy defaults, this
object requires using the following protocol:
    Call at least once set_algo(hasher_algo, hasher_custom_exec=None) to choose
    the hashing algorithm.
    (This may be called multiple times with different arguments, before any call
    to an hashing method.)

    Then call hashing methods as required.

    Once an hashing methods have been called, no further calls to set_algo may
    be made.
"""

# Hashing imports are conditionally imported.
# pylint: disable=import-outside-toplevel

import os
import subprocess
from enum import IntEnum

import lnsync_pkg.printutils as pr
from lnsync_pkg.thread_utils import ProducerConsumerThreaded, NoMoreData
from lnsync_pkg.miscutils import uint64_to_int64, HelperAppError

class HasherFunction(IntEnum):
    """
    Enum which mathematical hashing function.
    """
    XXHASH32 = 0 # Default.
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

    def is_full_content_hasher(self):
        """
        Some hash algorithms use the full contents, and therefore different
        size inputs are expected to produce different hash values.
        """
        if self == HasherFunction.CUSTOM:
            return False
        else:
            return True

    def __str__(self):
        return self.name

class HasherAlgo(IntEnum):
    """
    Which specific hashing algorithm and library is used.
    """
    PYHASHXX = 0
    XXHASH32 = 1
    XXHASH64 = 2
    CUSTOM = 15

    @staticmethod
    def get_values():
        return [e.name for e in HasherAlgo if e.name != "CUSTOM"]

    def __str__(self):
        return self.name

class BlockHasher:
    _PRESET_HASHER_ALGO = None
    _PRESET_FILTER_ALGO = None
    _PRESET_HASHER_CUSTOM_EXEC = None
    HASHER_ALGO = None
    HASHER_CUSTOM_EXEC = None
    FILTER_EXEC = None

    hasher_class = None

    @staticmethod
    def is_full_content_hasher():
        hash_algo = BlockHasher.get_algo() # Make choices permanent.
        if BlockHasher.FILTER_EXEC is not None:
            return False
        else:
            hash_fun = HasherFunction.from_hasher_algo(hash_algo)
            return hash_fun.is_full_content_hasher()

    @staticmethod
    def set_filter(filter_exec=None):
        raise NotImplementedError

    @staticmethod
    def hash_file(fpath):
        BlockHasher.get_algo()
        if BlockHasher.HASHER_ALGO is HasherAlgo.CUSTOM:
            res = BlockHasher._hash_file_custom_exec(fpath)
        elif not BlockHasher.FILTER_EXEC:
            with open(fpath, "rb") as infile:
                if os.path.getsize(fpath) >= ASYNC_SIZE_THRESH:
                    res = BlockHasher.hash_open_file_async(infile)
                else:
                    res = BlockHasher.hash_open_file_sync(infile)
        return res
#        else: # TODO eliminate or refactor TODO
#            try:
#                procres = subprocess.run(
#                    ["/bin/sh", filter_exec, fpath],
#                    check=True, capture_output=True)
#            except subprocess.CalledProcessError as exc:
#                msg = "failed hashing %s (%s)" % (fpath, procres.output)
#                raise RuntimeError(msg) from exc
#            try:
#                res = int(procres.stdout)
#            except ValueError as exc:
#                msg = "invalid output for $(%s %s): %s" % \
#                                (filter_exec, fpath, procres.stdout)
#                raise Exception(msg) from exc
#            return uint64_to_int64(res)

    @staticmethod
    def hash_open_file_async(infile):
        readerhasher = ReaderHasher(infile)
        return readerhasher.run()

    @staticmethod
    def hash_open_file_sync(infile):
        """
        Return the xxhash for given open file.
        """
        block_size = SYNC_BLOCK_SIZE
        hasher = BlockHasher.hasher_class()
        while True:
            datab = infile.read(block_size)
            if not datab:
                break
            hasher.update(datab)
        return BlockHasher.hasher_digest(hasher)


    @staticmethod
    def hash_data(*args):
        BlockHasher._set_algo()
        return BlockHasher.hash_data(*args)

    @staticmethod
    def hasher_digest(*args):
        BlockHasher._set_algo()
        return BlockHasher.hasher_digest(*args)

    @staticmethod
    def set_algo(hasher_algo, hasher_custom_exec=None):
        assert isinstance(hasher_algo, HasherAlgo), \
            f"set_algo: not an algo: {hasher_algo}"
        if BlockHasher.HASHER_ALGO is not None:
            raise RuntimeError("blockhasher: inconsistent hasher choice")
        if hasher_algo == HasherAlgo.CUSTOM:
            if hasher_custom_exec is None:
                raise RuntimeError("blockhasher: missing custom exec")
        else:
            if hasher_custom_exec is not None:
                raise RuntimeError("blockhasher: unexpected custom exec")
        if BlockHasher._PRESET_HASHER_ALGO is not None:
            if BlockHasher._PRESET_HASHER_ALGO != hasher_algo:
                pr.warning("changing hasher from %s to %s" % \
                           (BlockHasher._PRESET_HASHER_ALGO, hasher_algo))
            elif hasher_algo == HasherAlgo.CUSTOM \
                and BlockHasher._PRESET_HASHER_CUSTOM_EXEC != hasher_custom_exec:
                # TODO: use canonical file path for exec
                pr.warning(
                    "changing hasher exec from %s to %s" % \
                    (BlockHasher._PRESET_HASHER_CUSTOM_EXEC, hasher_custom_exec))
        BlockHasher._PRESET_HASHER_ALGO = hasher_algo
        BlockHasher._PRESET_HASHER_CUSTOM_EXEC = hasher_custom_exec

    @staticmethod
    def _set_algo():
        """
        Actually set the hashing methods.
        """
    #    import pdb; pdb.set_trace()
    # TODO custom hasher exec.
        if BlockHasher._PRESET_HASHER_ALGO is None:
            raise RuntimeError("blockhasher: no hasher set")
        if BlockHasher.HASHER_ALGO is not None:
            raise RuntimeError("blockhasher: hasher already set")
        BlockHasher.HASHER_ALGO = BlockHasher._PRESET_HASHER_ALGO
        BlockHasher.HASHER_CUSTOM_EXEC = BlockHasher._PRESET_HASHER_CUSTOM_EXEC
        if BlockHasher.HASHER_ALGO == HasherAlgo.PYHASHXX:
            try:
                import pyhashxx # Returns int32
            except Exception:
                raise RuntimeError("missing pyhashxx module")
            BlockHasher.hash_data = pyhashxx.hashxx
            BlockHasher.hasher_class = pyhashxx.Hashxx
            def hasher_digest(obj):
                return obj.digest()
            BlockHasher.hasher_digest = hasher_digest
        elif BlockHasher.HASHER_ALGO == HasherAlgo.XXHASH64:
            import xxhash # This is a build dependency.
            def hash_data(data):
                return uint64_to_int64(xxhash.xxh64_intdigest(data))
            BlockHasher.hash_data = hash_data
            BlockHasher.hasher_class = xxhash.xxh64
            def hasher_digest(hasher):
                return uint64_to_int64(hasher.intdigest())
            BlockHasher.hasher_digest = hasher_digest
        elif BlockHasher.HASHER_ALGO == HasherAlgo.XXHASH32:
            import xxhash
            BlockHasher.hash_data = xxhash.xxh32_intdigest
            BlockHasher.hasher_class = xxhash.xxh32
            def hasher_digest(hasher):
                return hasher.intdigest()
            BlockHasher.hasher_digest = hasher_digest
        elif BlockHasher.HASHER_ALGO == HasherAlgo.CUSTOM:
            BlockHasher.hash_open_file_sync = BlockHasher._raise_runtime_error
            BlockHasher.hash_open_file_async = BlockHasher._raise_runtime_error

    @staticmethod
    def _raise_runtime_error(*args, **kwargs):
        raise RuntimeError()

    @staticmethod
    def _hash_file_custom_exec(abspath):
        try:
            cmd = [BlockHasher.HASHER_CUSTOM_EXEC, abspath]
            procres = subprocess.run(
                cmd,
                capture_output=True, check=True)
        except subprocess.CalledProcessError as exc:
            msg = "failed hashing: %s (%s)." % (abspath, procres.stderr, )
            raise HelperAppError(cmd, msg) from exc
        try:
            res = int(procres.stdout)
        except ValueError as msg:
            msg = "invalid hasher output for %s (%s)." % \
                    (abspath, procres.stdout)
            raise RuntimeError(msg) from exc
        return uint64_to_int64(res)

    @staticmethod
    def get_algo():
        if BlockHasher.HASHER_ALGO is None:
            BlockHasher._set_algo()
        return BlockHasher.HASHER_ALGO

ASYNC_SIZE_THRESH = 512 * 2**20 # Files larger than 512 Mib
                                # are processed asynchronously, with threads.
SYNC_BLOCK_SIZE = 4 * 2**20   # 4 MiB blocks at a time for small files.
ASYNC_BLOCK_SIZE = 16 * 2**20  # 16 MiB blocks for large files.

class ReaderHasher(ProducerConsumerThreaded):
    """
    Read and hash in parallel threads.
    """
    def __init__(self, infile):
        self.hasher = BlockHasher.hasher_class()
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
        return BlockHasher.hasher_digest(self.hasher)

if __name__ == "__main__":
    import sys
    BlockHasher.set_algo(HasherAlgo.XXHASH32)
    if len(sys.argv) != 2:
        raise SystemExit("usage: blockhash <filepath> or blockhash -")
    PARAM = sys.argv[1]
    if PARAM == "-":
        RES = BlockHasher.hash_open_file_sync(sys.stdin.buffer)
    else:
        RES = BlockHasher.hash_file(PARAM)
    print(RES)
