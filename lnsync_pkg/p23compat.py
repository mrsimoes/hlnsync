#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Compatibility layer to support both Python 2 and 3.

The facts of life:

Python string-like types have changed from:
    Python 2: str <- basestring -> unicode
to
    Python 3: bytes <- object -> str

We a need string-like type represent file and dir names, above all else.

Python 3 believes that Unix file and dir names are binary strings that are
_supposed_ to encode a readable string via the scheme obtainable at
sys.getfilesystemencoding().

Therefore, Python3 is eager to convert file and dir names to its nice,
universal, encoding-agnostic str type.

But it can easily happen that a filename is invalid as a text encoding in the
sys.getfilesystemencoding() scheme (e.g. the system locale is en_US.UTF-8,
but a filename may contain invalid codepoints.)

Therefore, Pyuthon 3 adopted a reversible scheme to convert from Unix file
names to it's str via so-called surrogates.  (See PEP383 Non-decodable Bytes
in System Character Interfaces)

In particular, Python 3 presents mangled Unix file names as argv. To unmangle:
    fse = sys.getfilesystemencoding()
    filename.encode(fse, "surrogateescape") # pass to binary

File Path Encoding: Unix argv parameters:
    In C: either coded using LOCALE encoding (if fully input by the user) or in
direct binary if from glob or shell tab-completion.
    In Python2: as type str, in LOCALE encoding.
    In Python3: as type str (Unicode) with possibly embedded surrogates for
illegal UTF-8 sequences in the original. This allows recovering the exact
original C argv binary sequence filename.

Python 3 Standard Library will often accept either Unix or str file names,
though not mixed, and will return their results in the same representation.

(sqlite3.connect insists on a str file name, though.)

We need a data type used only to represent Unix file and dir names, which we
never process or manipulate as text (e.g. capitalize, reverse, sort).

The choice here was to keep the original byte sequence, unmangling the
appropriate Python-generated argv values at the entry point. Everywhere a file
name is used (including dir separator characters and empty path names) the
value is explicitly coerced to our file name type.

Some other advantages:
    - Data representation is exactly the same in both Python versions.
    - Python str strings with surrogate encodings are not printable, so they
    must be converted somehow, while the original filenames are always handled
    by the surrounding shell.

TODO: make use of the faster Python 3 os.fsdecode, os.scandir and Dir objects.
TODO: define a bona-fide fstr type.
TODO: make explicit the conversion from fstr to database output, for
compatibility.
"""

import sys
import six

FSE = sys.getfilesystemencoding() # Always UTF8 on Linux.

if six.PY2:
    import itertools
    zip = itertools.izip
    imap = itertools.imap
    sql_text_factory = str
    sql_text_storer = lambda x: buffer(x)
    def fstr(value):
        """Create an fstr from None, str by passing through."""
        if value is None:
            return None
        elif isinstance(value, buffer):
            return str(value)
        else:
            assert isinstance(value, str)
            return value
    def fstr2str(value):
        "Convert to str type."
        return value
    def isfstr(value):
        return isinstance(value, str)
else:
    zip = zip # Builtins.
    imap = map
    sql_text_factory = bytes
    sql_text_storer = lambda x: x
    def fstr(value):
        """Create an fstr from None, str or bytes."""
        global FSE
        if value is None:
            return None
        elif isinstance(value, str):
            return value.encode(FSE, "surrogateescape")
        elif isinstance(value, bytes):
            return value
        else:
            assert False, "unknown value"
    def fstr2str(value):
        "Convert to str type."
        try:
            st = value.decode(FSE)
        except UnicodeDecodeError as exc:
#            pr.error(exc)
            st = value.decode(FSE,'replace')
        return st
    def isfstr(value):
        return isinstance(value, bytes)
