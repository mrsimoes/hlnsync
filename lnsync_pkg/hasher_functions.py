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
Built-in hashing functions and algorithms.

Values returned are always converted to *signed* int64.

The main hashing algorithm are xxhash variants:
    - xxhash: supports xxhash32 as well as a few other hasher functions.
    - pyhashxx: implements xxhash32, which returns a 32-bit value.
    This was the only choice in earlier versions. It requires compilation
    when installing via pip on Windows.
(For large files (>1 GiB), use a reader/hasher thread combo.)
"""

# Hashing imports are conditionally imported.
# pylint: disable=import-outside-toplevel

import os
import abc
import subprocess
from enum import IntEnum

import lnsync_pkg.printutils as pr
from lnsync_pkg.miscutils import uint64_to_int64, HelperAppError
from lnsync_pkg.thread_utils import ProducerConsumerThreaded, NoMoreData

# Hashing imports are conditionally imported.
# pylint: disable=import-outside-toplevel

class HasherFunctionID(IntEnum):
    """
    Each subclass of FileHasherAlgo should declare its value in the
    _hasher_function_id attrribute.

    Value is stored in the db in a 8-bit field, so restrict values to 0-255.
    """
    XXHASH32 = 0 # Default.
    XXHASH64 = 1
    IMAGE_DHASH = 2
    THUMB_DHASH = 3
    THUMB_DHASH_SYM = 4
    EXTERNAL = 255

    @staticmethod
    def get_values():
        return [e.name for e in HasherFunctionID if e.name != "EXTERNAL"]

    def __str__(self):
        return self.name

class FileHasherAlgo:
    """
    Abstract base class for all file hashers.
    """

    _hasher_function_id = None
    _subclass_registry = {}

    def __init_subclass__(cls, **kwargs):
        hasher_id = cls._hasher_function_id
        if hasher_id is not None:
            assert isinstance(cls._hasher_function_id, HasherFunctionID), \
                   f"Bad hasher function id for {cls}"
            if hasher_id in cls._subclass_registry:
                raise RuntimeError(f"Duplicate hasher id: {hasher_id}")
            cls._subclass_registry[hasher_id] = cls
        super().__init_subclass__(**kwargs)

    @staticmethod
    def hasher_from_id(algo_id):
        return FileHasherAlgo._subclass_registry[algo_id]

    @abc.abstractmethod
    def hash_file(self, fpath):
        pass

    @classmethod
    def hash_depends_on_file_size(cls):
        """
        Some hash algorithms use the full file contents, and so different
        size inputs are expected to produce different hash values.
        """

    def get_hasher_function_id(self):
        return self._hasher_function_id

    def get_hasher_algo_id_plus(self):
        "Plus parameters."
        return (self._hasher_function_id,)

class DigestHasher(FileHasherAlgo):
    """
    Hashers that do not depend on file size.
    """
    @classmethod
    def hash_depends_on_file_size(cls):
        return False

class ImageHasher(DigestHasher):
    _hasher_function_id = HasherFunctionID.IMAGE_DHASH

    def __init__(self):
        try:
            from lnsync_pkg.image_dhash import dhash
        except Exception as exc:
            msg = f"Cannot load PIL module: {str(exc)}"
            raise RuntimeError(msg) from exc
        self.dhash = dhash

    def hash_file(self, fpath):
        dhash_val = self.dhash(fpath)
        return dhash_val

class ThumbnailHasher(ImageHasher):
    _hasher_function_id = HasherFunctionID.THUMB_DHASH

    def __init__(self):
        super().__init__()
        try:
            from lnsync_pkg.gnome_thumbnailer import GnomeThumbnailer
        except Exception as exc:
            msg = f"cannot load gnome thumbnail module: {str(exc)}"
            raise RuntimeError(msg) from exc
        self.gnome_thumbnailer = GnomeThumbnailer()

    def hash_file(self, fpath):
        thumbnail_path = self.gnome_thumbnailer.make_thumbnail(fpath)
        dhash_val = self.dhash(thumbnail_path)
        return dhash_val

class ThumbnailMirrorHasher(ThumbnailHasher):
    _hasher_function_id = HasherFunctionID.THUMB_DHASH_SYM

    def __init__(self):
        super().__init__()
        try:
            from lnsync_pkg.image_dhash import dhash_symmetric
        except Exception:
            raise RuntimeError("Cannot load PIL module")
        self.dhash = dhash_symmetric

class ExternalHasher(DigestHasher):
    _hasher_function_id = HasherFunctionID.EXTERNAL

    def __init__(self, external_exec):
        self.external_exec = external_exec

    def get_hasher_algo_id_plus(self):
        return (self._hasher_function_id, self.external_exec)

    def hash_file(self, fpath):
        abspath = fpath
        try:
            cmd = [self.external_exec, abspath]
            procres = subprocess.run(
                cmd,
                capture_output=True, check=True)
        except subprocess.CalledProcessError as exc:
            msg = "failed hashing: %s (%s)." % (abspath, procres.stderr, )
            raise HelperAppError(cmd, msg) from exc
        try:
            res = int(procres.stdout)
        except ValueError as msg:
            msg = f"invalid hasher output for {abspath} ({procres.stdout})."
            raise RuntimeError(msg) from exc
        return uint64_to_int64(res)


ASYNC_SIZE_THRESH = 512 * 2**20 # Files larger than 512 Mib
                                # are processed asynchronously, with threads.
SYNC_BLOCK_SIZE = 4 * 2**20   # 4 MiB blocks at a time for small files.
ASYNC_BLOCK_SIZE = 16 * 2**20  # 16 MiB blocks for large files.

class FileBlockHasher(FileHasherAlgo):
    """
    Hash large disk files in blocks, synchronously or asynchronously.
    """
    _hasher_engine_class = None
    _hasher = None
    _filter_exec = None

    def __init__(self, *args):
        super().__init__(*args)
        assert self._hasher_engine_class, "missing hasher engine class"
        self._hasher = self._hasher_engine_class()

    @classmethod
    def hash_depends_on_file_size(cls):
        if cls._filter_exec is not None:
            return False
        else:
            return True

    def hash_datum(self, datum):
        return self._hasher.hash_datum(datum)

    def hash_file(self, fpath):
        if not self._filter_exec:
            with open(fpath, "rb") as infile:
                if os.path.getsize(fpath) >= ASYNC_SIZE_THRESH:
                    res = self.hash_open_file_async(infile)
                else:
                    res = self.hash_open_file_sync(infile)
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

    def hash_open_file_sync(self, infile):
        block_size = SYNC_BLOCK_SIZE
        hasher = self._hasher
        hasher.reset()
        while True:
            datum = infile.read(block_size)
            if not datum:
                break
            hasher.update(datum)
        return hasher.digest()

    def hash_open_file_async(self, infile):
        self._hasher.reset()
        readerhasher = ReaderHasher(self._hasher, infile)
        return readerhasher.run()

class ReaderHasher(ProducerConsumerThreaded):
    """
    Read and hash in parallel threads.
    """
    def __init__(self, hasher, infile):
        """
        hasher is an object with methods:
            update(datum)
            digest() -> value
        infile is an open file
        """
        self.hasher = hasher
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

class HasherEngine:
    """
    Abstract base class for progressive hasher engines.
    """
    @abc.abstractmethod
    def reset(self):
        pass

    @abc.abstractmethod
    def update(self, datum):
        pass

    @abc.abstractmethod
    def digest(self):
        pass

    @abc.abstractmethod
    def hash_datum(self, datum): pass

class FileHasherXXHASH32(FileBlockHasher):
    _hasher_function_id = HasherFunctionID.XXHASH32

    class XXHASH32Engine(HasherEngine):
        def __init__(self):
            import xxhash # This is a build dependency.
            self.hasher = xxhash.xxh32()
            self.xxhash = xxhash

        def reset(self):
            self.hasher.reset()

        def update(self, datum):
            self.hasher.update(datum)

        def digest(self):
            return self.hasher.intdigest()

        def hash_datum(self, datum):
            return self.xxhash.xxh32_intdigest(datum)

    _hasher_engine_class = XXHASH32Engine

class FileHasherXXHASH64(FileBlockHasher):

    _hasher_function_id = HasherFunctionID.XXHASH64

    class XXHASH64Engine(HasherEngine):

        def __init__(self):
            import xxhash # This is a build dependency.
            self.hasher = xxhash.xxh64()
            self.xxhash = xxhash

        def reset(self):
            self.hasher.reset()

        def update(self, datum):
            self.hasher.update(datum)

        def digest(self):
            return uint64_to_int64(self.hasher.intdigest())

        def hash_datum(self, datum):
            return uint64_to_int64(self.xxhash.xxh64_intdigest(datum))

    _hasher_engine_class = XXHASH64Engine

class HasherManager:
    """
    Global settings, for all module clients.
    """
    _hasher = None
    _using_default = True

    @staticmethod
    def reset(): # TODO rename to avoid confusion with Hasher.reset
        HasherManager.set_hasher(FileHasherXXHASH32)
        HasherManager._using_default = True

    @staticmethod
    def set_filter(filter_exec=None):
        raise NotImplementedError

    @staticmethod
    def get_hasher():
        return HasherManager._hasher

    @staticmethod
    def get_hasher_function_id():
        return HasherManager._hasher.get_hasher_function_id()

    @staticmethod
    def set_hasher(hasher_spec, *args):
        """
        Accept a HasherFunctionID, a FileHasherAlgo instance, or a class derived
        from FileHasherAlgo.
        """
        if isinstance(hasher_spec, HasherFunctionID):
            hasher_class = FileHasherAlgo.hasher_from_id(hasher_spec)
            hasher = hasher_class(*args)
        elif isinstance(hasher_spec, FileHasherAlgo):
            hasher = hasher_spec
        elif isinstance(hasher_spec, type) \
             and issubclass(hasher_spec, FileHasherAlgo):
            hasher = hasher_spec(*args)
        else:
            msg = f"not a FileHasherAlgo type or instance: {hasher_spec}"
            raise RuntimeError(msg)
        if HasherManager._hasher:
            old_algo = HasherManager.get_hasher().get_hasher_algo_id_plus()
            new_algo = hasher.get_hasher_algo_id_plus()
            if  old_algo != new_algo:
                pr.warning(f"changing hasher from {old_algo} to {new_algo}")
        HasherManager._hasher = hasher

HasherManager.reset()

def main():
    import sys
    hasher = FileHasherXXHASH32()
    if len(sys.argv) != 2:
        raise SystemExit(
            "usage: hasher_functions <filepath> or hasher_functions -")
    param = sys.argv[1]
    if param == "-":
        res = hasher.hash_open_file_sync(sys.stdin.buffer)
    else:
        res = hasher.hash_file(param)
    print(res)

if __name__ == "__main__":
    main()
