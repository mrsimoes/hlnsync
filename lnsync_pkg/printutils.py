#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Provide printing service with verbosity-controlled output levels
and same-line progress messages.
Output levels control
- fatal, error and warning output to stderr at verbosity levels >= -2,-1,0 resp.
- print, info, and debug, trace output to stdout at
  verbosity levels >= 0, 1, 2, and 3 resp.
- progress outputs to stdout if stdout is a tty and verbosity is >= 0.
Default verbosity is 0, but can be set negative.
(See https://www.xfree86.org/4.8.0/ctlseqs.html for terminal control sequences.)
A context may be setup during which a prefix is prepended to progress messages.
These contexts may be nested.
A prefix may be set for each non-progress message.
"""

import sys
import atexit
from itertools import chain
from collections import defaultdict
import threading

from lnsync_pkg.p23compat import isfstr

# Make printing sequences of terminal ctrl codes thread-safe.
PRINT_LOCK = threading.RLock()

FATAL_LEVEL = -2
ERROR_LEVEL = -1
WARNING_LEVEL = 0
PRINT_LEVEL = 0
INFO_LEVEL = 1
DEBUG_LEVEL = 2
TRACE_LEVEL = 3
PROGRESS_LEVEL = 0

# Tell pylint not to mistake module variables for constants
# pylint: disable=C0103

# Set by the module user.
option_verbosity = 0

_print = print
_stdout_is_tty = sys.stdout.isatty()
_stderr_is_tty = sys.stderr.isatty()

_progress_was_printed = False
_thread_prog_prefix_stack = defaultdict(lambda : [])
_thread_prog_prefix = defaultdict(lambda : "")

_app_prefix = ""

class ProgressPrefix:
    """
    Set up a context during which a prefix is prepended to progress output.
    """
    def __init__(self, prefix):
        self.we = threading.get_ident()
        we = self.we
        with PRINT_LOCK:
            _thread_prog_prefix_stack[we].append(prefix)
            _thread_prog_prefix[we] = "".join(_thread_prog_prefix_stack[we])

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        we = self.we
        with PRINT_LOCK:
            _thread_prog_prefix_stack[we].pop()
            _thread_prog_prefix[we] = "".join(_thread_prog_prefix_stack[we])
        return False # We never handle exceptions.

def progress(*args):
    """
    Print each arg in the same line, without changing to the next line.
    """
    global _progress_was_printed
    global option_verbosity
    if option_verbosity < 0 or not _stdout_is_tty:
        return
    with PRINT_LOCK:
        we = threading.get_ident()
        _progress_prefix = _thread_prog_prefix[we]
        line = _progress_prefix + "".join(args)
        line = line.replace("\n", "")
        _print('\033[?7l', end="") # Wrap off.
        _print(line, end="\033[0K\r") # Erase to end of line.
        _print('\033[?7h', end="") # Wrap on.
        _progress_was_printed = True
        sys.stdout.flush()

def progress_percentage(par, tot):
    """
    Print the percentage correspondint to par out of tot items.
    """
    perc = 100 * par // tot
    progress("%02d%%" % (perc,))

def set_app_prefix(pref):
    global _app_prefix
    _app_prefix = pref

def _print_main(*args, **kwargs):
    global _progress_was_printed
    file = kwargs.pop("file", sys.stdout)
    end = kwargs.pop("end", "\n")
    assert not kwargs
    assert file in (sys.stdout, sys.stderr)
    if file == sys.stdout:
        is_tty = _stdout_is_tty
    else:
        is_tty = _stderr_is_tty
    with PRINT_LOCK:
        if is_tty and _progress_was_printed:
            _print("\r\033[2K", end="") # Erase current line.
    #    for line in "".join(map(str, args)).splitlines():
    #        _print(line, file=file, **kwargs)
        for arg in args:
            if True or not isfstr(arg):
                arg = str(arg)
            file.write(arg)
        if end:
            file.write(end)
        file.flush()
        _progress_was_printed = False

def print(*args, **kwargs):
    if option_verbosity >= PRINT_LEVEL:
        _print_main(*args, file=sys.stdout, **kwargs)

def fatal(*args, **kwargs):
    if option_verbosity >= FATAL_LEVEL:
        _print_main(_app_prefix, "fatal: ", *args, file=sys.stderr, **kwargs)

def error(*args, **kwargs):
    with PRINT_LOCK:
        if option_verbosity >= ERROR_LEVEL:
            try:
                if _stderr_is_tty:
                    _print("\033[31m", file=sys.stderr, end="") # Red forgr.
                _print_main(_app_prefix,
                            "error: ", *args, file=sys.stderr, **kwargs)
            finally:
                if _stderr_is_tty:
                    _print("\033[39m", file=sys.stderr, end="") # Std foreg.
                    sys.stderr.flush()

def info(*args, **kwargs):
    if option_verbosity >= INFO_LEVEL:
        _print_main(*args, file=sys.stdout, **kwargs)

def warning(*args, **kwargs):
    with PRINT_LOCK:
        if option_verbosity >= WARNING_LEVEL:
            try:
                if _stderr_is_tty:
                    _print("\033[33m", file=sys.stderr, end="") # Red forgr.
                _print_main(_app_prefix,
                            "warning: ", *args, file=sys.stderr, **kwargs)
            finally:
                if _stderr_is_tty:
                    _print("\033[39m", file=sys.stderr, end="") # Std foreg.
                    sys.stderr.flush()

def debug(template_str, *str_args, **kwargs):
    """Templace with % placeholders and respective are given separately."""
    if option_verbosity >= DEBUG_LEVEL:
        _print_main("debug: ",
                    template_str % str_args, file=sys.stderr, **kwargs)

def trace(template_str, *str_args, **kwargs):
    """Templace with % placeholders and respective are given separately."""
    if option_verbosity >= TRACE_LEVEL:
        _print_main("trace: ",
                    template_str % str_args, file=sys.stderr, **kwargs)

def _exit_func():
    with PRINT_LOCK:
        if _stdout_is_tty:
            _print("\033[0J", end="") # Clear line.
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
        _print("\nverbosity %d" % v)
        error(msg + "error")
        _print(msg + "print")
        info(msg + "info")
        warning(msg + " warning")
        trace("%s debug ", msg)
        trace("%s trace", msg)
    for k in range(20):
        for it in sys.argv[1:]:
            progress(it * random.randint(1, 5))
            time.sleep(0.5)
