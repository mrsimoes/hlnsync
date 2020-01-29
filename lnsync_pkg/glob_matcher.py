#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Implement a glob pattern to match files and dirs, as in rsync --exclude
and --include, in a manner well-suited to recursive file tree walking.

Each pattern is a list of one or more basename glob pattern strings, with
optional elements anchoring head '/' and dir matching trailing indicator '/'.

From the rsync man page:
   o  if the pattern starts with a / then it is anchored to a particular spot  in  the  hierarchy  of
      files, otherwise it is matched against the end of the pathname.  This is similar to a leading ^
      in regular expressions.  Thus "/foo" would match a name of "foo" at either  the  "root  of  the
      transfer"  (for a global rule) or in the merge-file's directory (for a per-directory rule).  An
      unqualified "foo" would match a name of "foo" anywhere in the tree  because  the  algorithm  is
      applied  recursively  from  the  top  down; it behaves as if each path component gets a turn at
      being the end of the filename.  Even the unanchored "sub/foo" would match at any point  in  the
      hierarchy where a "foo" was found within a directory named "sub".  See the section on ANCHORING
      INCLUDE/EXCLUDE PATTERNS for a full discussion of how to specify a pattern that matches at  the
      root of the transfer.
   o  if  the pattern ends with a / then it will only match a directory, not a regular file, symlink,
      or device.
   o  rsync chooses between doing a simple string match and wildcard matching by checking if the pat-
      tern contains one of these three wildcard characters: '*', '?', and '['.
   o  a '*' matches any path component, but it stops at slashes.
   o  use '**' to match anything, including slashes.
   o  a '?' matches any character except a slash (/).
   o  a '[' introduces a character class, such as [a-z] or [[:alpha:]].
   o  in  a  wildcard  pattern,  a  backslash  can  be used to escape a wildcard character, but it is
      matched literally when no wildcards are present.  This means that there is an  extra  level  of
      backslash  removal  when  a pattern contains wildcard characters compared to a pattern that has
      none.  e.g. if you add a wildcard to "foo\bar" (which matches the backslash) you would need  to
      use "foo\\bar*" to avoid the "\b" becoming just "b".
   o  if  the  pattern contains a / (not counting a trailing /) or a "**", then it is matched against
      the full pathname, including any leading directories. If the pattern doesn't contain a /  or  a
      "**",  then it is matched only against the final component of the filename.  (Remember that the
      algorithm is applied recursively so "full filename" can actually be any portion of a path  from
      the starting directory on down.)
   o  a  trailing "dir_name/***" will match both the directory (as if "dir_name/" had been specified)
      and everything in the directory (as if "dir_name/**" had been specified).   This  behavior  was
      added in version 2.6.7.
"""

import os
import fnmatch

from lnsync_pkg.p23compat import fstr

def path_full_split(path):
    """Split a matching pattern into a list of components.
    If the pattern starts with /, the first element of the result is "/"
    If the pattern ends with /, the last element of the result is ""."""
    terms = []
    while True:
        path, term = os.path.split(path)
        terms.append(term)
        if path == fstr("/"):
            terms.append(path)
            break
        if path == fstr(""):
            break
    terms.reverse()
    return terms

class Pattern(object):
    __slots__ = ("_split_pattern", "_type")
    def __init__(self, glob_pattern_path, pattern_type):
        self._split_pattern = path_full_split(glob_pattern_path)
        self._type = pattern_type

    def is_singleton(self):
        pattern = self._split_pattern
        extra_len = 0
        if pattern[0] == fstr("/"):
            extra_len += 1
        if pattern[-1] == fstr(""):
            extra_len += 1
        return len(pattern) == 1 + extra_len

    def is_anchored(self):
        pattern = self._split_pattern
        return pattern[0] == fstr("/")

    def is_dir_matcher(self):
        pattern = self._split_pattern
        return pattern[-1] == fstr("")

    def head_matches(self, basename):
        first_term = self.get_head()
        return fnmatch.fnmatch(basename, first_term)

    def get_head(self):
        "Return a term in the pattern, not a pattern."
        pattern = self._split_pattern
        if pattern[0] == fstr("/"):
            head = pattern[1]
        else:
            head = pattern[0]
        return head

    def get_tail(self):
        "Return the tail pattern."
        assert not self.is_singleton(), "get_tail"
        new_patt = Pattern(fstr(""), self._type)
        pattern = self._split_pattern
        if pattern[0] == fstr("/"):
            new_patt._split_pattern = pattern[2:]
        else:
            new_patt._split_pattern = pattern[1:]
        return new_patt

    def is_exclude(self):
        return self._type == "e"

    def is_include(self):
        return self._type == "i"

    def to_fstr(self):
        res = fstr(os.sep).join(self._split_pattern)
        if res[0:2] == fstr("//"):
            res = res[1:]
        if res[-2:-1] == fstr("//"):
            res = res[:-1]
        return res

class ExcludePattern(Pattern):
    def __init__(self, glob_pattern_path):
        super(ExcludePattern, self).__init__(glob_pattern_path, "e")

class IncludePattern(Pattern):
    def __init__(self, glob_pattern_path):
        super(IncludePattern, self).__init__(glob_pattern_path, "i")

class GlobMatcher(object):
    """Match relative filenames to a list of glob patterns and create subdir
    matchers in a recursive-friendly way.
    """
    __slots__ = ["_patterns"]

    def __init__(self, patterns=None):
        """patterns is a list of (tag, glob pattern string).
        """
        if patterns is None:
            patterns = []
        assert isinstance(patterns, list)
        self._patterns = patterns

    def all_patterns_iter(self):
        return self._patterns

    def match_file_bname(self, file_bname):
        """Return True if the single basename file_bname matches some exclude
        pattern before matching any include pattern.
        """
        res = False
        for pat in self.all_patterns_iter():
            if not pat.is_dir_matcher() and pat.head_matches(file_bname):
                if pat.is_include():
                    res = False
                else:
                    res = True
                break
        return res

    def match_dir_bname(self, dir_bname):
        """Return True if dir_bname matches no pattern or matches some include
        pattern before matching any exclude pattern."""
        res = False
        for pat in self.all_patterns_iter():
            if pat.head_matches(dir_bname):
                if pat.is_include():
                    res = False
                else:
                    res = True
                break
        return res

    def to_subdir(self, dir_bname):
        """Return a GlobMatcher representing patterns to use one directory down.
        """
        subdir_patterns = []
        for pat in self.all_patterns_iter():
            if not pat.is_anchored():
                subdir_patterns.append(pat)
            if not pat.is_singleton() and pat.head_matches(dir_bname):
                subdir_patterns.append(pat.get_tail())
        if subdir_patterns:
            new_glob_matcher = GlobMatcher([])
            new_glob_matcher._patterns = subdir_patterns
            res = new_glob_matcher
        else:
            res = None
        return res
