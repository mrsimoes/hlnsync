#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Implements a glob pattern to match files and dirs, as in rsync --exclude
patterns, with methods well-suited to recursive filetree walking.

Each pattern is a list of intermediate glob patterns, each already split into a list of
path components.
Each pattern is a list of one or more basename glob pattern strings, plus an optional
head '/' entry and/or an optional trailing '' entry, interpreted as in rsync --exclude.
"""

import os
from itertools import chain
import fnmatch

def path_totalsplit(path):
    folders = []
    while True:
        path, folder = os.path.split(path)
        folders.append(folder)
        if path == "/":
            folders.append(path)
            break
        if path == "":
            break
    folders.reverse()
    return folders

def pattern_is_singleton(pattern):
    extra_len = 0
    if pattern[0] == "/":
        extra_len += 1
    if pattern[-1] == "":
        extra_len += 1
    return len(pattern) == 1 + extra_len

def pattern_is_anchored(pattern):
    return pattern[0] == "/"

def pattern_is_dir_matcher(pattern):
    return pattern[-1] == ""

def pattern_head_matches(pattern, basename):
    if pattern[0] == "/":
        first_term = pattern[1]
    else:
        first_term = pattern[0]
    return fnmatch.fnmatch(basename, first_term)

def pattern_head(pattern):
    if pattern[0] == "/":
        return pattern[1]
    else:
        return pattern[0]

def pattern_tail(pattern):
    assert not pattern_is_singleton(pattern)
    if pattern[0] == "/":
        return pattern[2:]
    else:
        return pattern[1:]

class GlobMatcher(object):
    """Match relative filenames to a list of glob patterns and create subdir
    matchers in a recursive-friendly way.
    """

    # patterns_permanent is a read-only list, shareable with other instances.
    # They're the initial non-anchored patterns.
    __slots__ = ["patterns_permanent", "patterns_volatile"]

    def __init__(self, patterns=None):
        """patterns is a list of glob pattern string.
        """
        if patterns is None:
            patterns = []
        assert isinstance(patterns, list)
        patterns = map(path_totalsplit, patterns)
        def not_anchored(p):
            return not pattern_is_anchored(p)
        self.patterns_permanent = filter(not_anchored, patterns)
        self.patterns_volatile = filter(pattern_is_anchored, patterns)

    def all_patterns_iter(self):
        return chain(self.patterns_permanent, self.patterns_volatile)

    def match_file_bname(self, file_bname):
        """Return True if the single basename file_bname matches some pattern.
        """
        def matches(pat):
            return not pattern_is_dir_matcher(pat) \
                and pattern_head_matches(pat, file_bname)
        return any(matches(p) for p in self.all_patterns_iter())

    def match_dir_bname(self, dir_bname):
        """Return True if dir_bname matches some pattern."""
        def matches(pat):
            return pattern_head_matches(pat, dir_bname)
        return any(matches(p) for p in self.all_patterns_iter())

    def to_subdir(self, dir_bname):
        """Return a GlobMatcher representing patterns to use one directory down.
        """
        subdir_volatile = []
        for p in self.all_patterns_iter():
            if not pattern_is_singleton(p) \
                    and pattern_head_matches(p, dir_bname):
                subdir_volatile.append(pattern_tail(p))
        if self.patterns_permanent or subdir_volatile:
            new_glob_matcher = GlobMatcher([])
            new_glob_matcher.patterns_permanent = self.patterns_permanent
            new_glob_matcher.patterns_volatile = subdir_volatile
            return new_glob_matcher
        else:
            return None
