#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""Provide printing service with verbosity-controlled output levels
and same-line progress messages.
print, info, and debug print to stdout at verbosity levels >= 0, 1, and 2.
error and warning print to stderr and verbosity levels >= 0, 1.
progress outputs to stdout if it is a tty and verbosity is >= 0.
Default verbosity is 0.
(See https://www.xfree86.org/4.8.0/ctlseqs.html for terminal control sequences.)
A context may be setup during which a prefix is prepended to progress messages.
These contexts may be nested.
A prefix may be set for each non-progress message.
"""

from __future__ import print_function

import sys
import atexit
from itertools import chain

# Tell pylint not to mistake module variables for constants
# pylint: disable=C0103

# Set by the module user.
option_verbosity = 0

_builtin_print = print
_stdout_is_tty = sys.stdout.isatty()
_stderr_is_tty = sys.stderr.isatty()

_progress_prefixes = []
_progress_was_printed = False

_app_prefix = ""

class ProgressPrefix(object):
    """Set up a context during which a prefix is prepended to progress output.
    """
    def __init__(self, prefix):
        self.prefix = prefix
    def __enter__(self):
        global _progress_prefixes
        _progress_prefixes.append(self.prefix)
    def __exit__(self, exc_type, exc_value, traceback):
        global _progress_prefixes
        assert _progress_prefixes[-1] == self.prefix
        _progress_prefixes.pop()
        return False # We never handle exceptions.

def progress(*args):
    """Print each arg in the same line, without changing to the next line."""
    global _progress_prefixes
    global _progress_was_printed
    global option_verbosity
    if option_verbosity < 0 or not _stdout_is_tty:
        return
    line = "".join(chain(_progress_prefixes, args))
    line = line.replace("\n", "")
    _builtin_print('\033[?7l', end="") # Wrap off.
    _builtin_print(line, end="\033[0K\r") # Erase to end of line.
    _builtin_print('\033[?7h', end="") # Wrap on.
    _progress_was_printed = True
    sys.stdout.flush()

def progress_percentage(par, tot):
    """Print the percentage correspondint to par out of tot items."""
    perc = 100 * par // tot
    progress("%02d%%" % (perc,))

def set_app_prefix(pref):
    global _app_prefix
    _app_prefix = pref

def _print_main(*args, **kwargs):
    global _progress_was_printed
    file = kwargs.pop("file", sys.stdout)
    assert file in (sys.stdout, sys.stderr)
    if file == sys.stdout:
        is_tty = _stdout_is_tty
    else:
        is_tty = _stderr_is_tty
    if is_tty and _progress_was_printed:
        _builtin_print("\r\033[2K", end="") # Erase current line.
    for line in "".join(map(str, args)).splitlines():
        _builtin_print(line, file=file, **kwargs)
    _progress_was_printed = False

def print(*args, **kwargs):
    if option_verbosity >= 0:
        _print_main(*args, file=sys.stdout, **kwargs)

def fatal(*args, **kwargs):
    if option_verbosity >= -1:
        _print_main(_app_prefix, "fatal: ", *args, file=sys.stderr, **kwargs)

def error(*args, **kwargs):
    if option_verbosity >= 0:
        if _stderr_is_tty:
            _builtin_print("\033[31m", file=sys.stderr, end="") # Red forgr.
        _print_main(_app_prefix, "error: ", *args, file=sys.stderr, **kwargs)
        if _stderr_is_tty:
            _builtin_print("\033[39m", file=sys.stderr, end="") # Std foreg.

def info(*args, **kwargs): # Default verbosity level is 1.
    if option_verbosity >= 1:
        _print_main(_app_prefix, *args, file=sys.stdout, **kwargs)

def warning(*args, **kwargs):
    if option_verbosity >= 2:
        _print_main(_app_prefix, "warning: ", *args, file=sys.stderr, **kwargs)

def debug(template_str, *str_args, **kwargs):
    """Templace with % placeholders and respective are given separately."""
    if option_verbosity >= 4:
        _print_main("debug: ",
                    template_str % str_args, file=sys.stderr, **kwargs)

def trace(template_str, *str_args, **kwargs):
    """Templace with % placeholders and respective are given separately."""
    if option_verbosity >= 5:
        _print_main("trace: ",
                    template_str % str_args, file=sys.stderr, **kwargs)

def _exit_func():
    _builtin_print("\033[0J", end="") # Clear line.
    try:     # Prevent "broken pipe" errors if outputs are closed before atexit.
        sys.stdout.flush()
        sys.stdout.close()
    except Exception:
        pass
    try:
        sys.stderr.flush()
        sys.stderr.close()
    except Exception:
        pass

atexit.register(_exit_func)

if __name__ == "__main__":
    import time
    import random
    if len(sys.argv) > 1:
        arg = str(sys.argv[1])
    else:
        arg = "TEST"
    msg = arg + " -- this is "
    set_app_prefix("myapp")
    for v in [-1, 0, 1, 2, 3, 4, 5]:
        option_verbosity = v
        _builtin_print("\nverbosity %d" % v)
        error(msg + "error")
        print(msg + "print")
        info(msg + "info")
        warning(msg + " warning")
        trace("%s debug ", msg)
        trace("%s trace", msg)
    for k in range(20):
        for it in sys.argv[1:]:
            progress(it * random.randint(1, 5))
            time.sleep(0.5)
