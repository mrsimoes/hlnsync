#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Implement a glob pattern to match files and dirs, as in rsync --exclude and
--include, in a way suited to recursive file tree walking.

Each pattern is a list of one or more basename glob pattern strings, with
optional elements anchoring head '/' and dir matching trailing indicator '/'.

The rsync ** matcher is supported.
"""

import fnmatch
from lnsync_pkg.p23compat import fstr

_SEP = fstr("/")
_STST = fstr("**")

class Pattern:
    __slots__ = ("_pattern", "_inner_str", "_type",
                 "_sep_pos", "_stst_pos",
                 "_anchored", "_dir_matcher")
    # _type is either "i" or "e"
    def __init__(self, glob_string, pattern_type=None):
        self._type = pattern_type
        if glob_string[:1] == _SEP:
            self._anchored = True
            glob_string = glob_string[1:]
        else:
            self._anchored = False
        if glob_string[-1:] == _SEP:
            self._dir_matcher = True
            glob_string = glob_string[:-1]
        else:
            self._dir_matcher = False
        self._inner_str = glob_string
        self._sep_pos = glob_string.find(_SEP)
        self._stst_pos = glob_string.find(_STST)

    def clone(self, new_inner_str):
        new_inner_str = self.to_fstr(inner_str=new_inner_str)
        new_pat = (type(self))(new_inner_str)
        return new_pat

    def to_fstr(self, inner_str=None):
        if inner_str is None:
            inner_str = self._inner_str
        prefix = _SEP if self.is_anchored() else fstr("")
        postfix = _SEP if self.is_dir_matcher() else fstr("")
        return prefix + inner_str + postfix

    def __hash__(self):
        return hash(self.to_fstr())

    def __eq__(self, other):
        return self.to_fstr() == other.to_fstr() \
                and self._type == other._type

    def is_anchored(self):
        return self._anchored

    def is_dir_matcher(self):
        return self._dir_matcher

    def is_empty(self):
        return len(self._inner_str) == 0

    def head_to_tails(self, component):
        """
        Return a list of new patterns that match the possible tail of path
        names for which the first component matches the given component.
        """
        tails = set()
        if 0 <= self._sep_pos < self._stst_pos \
                or self._stst_pos < 0 <= self._sep_pos:
            # There is some / before any **.
            if fnmatch.fnmatch(component, self._inner_str[:self._sep_pos]):
                tail_str = self._inner_str[self._sep_pos+1:]
                tails.add(self.clone(tail_str))
        elif 0 <= self._stst_pos:
            # There is some ** before any /.
            if self._sep_pos < 0:
                # No /: match everything.
                if fnmatch.fnmatch(component, self._inner_str):
                    tails.add(self.clone(fstr("")))
            else:
                # Some /: match everything up to  the /
                if fnmatch.fnmatch(component, self._inner_str[:self._sep_pos]):
                    tails.add(self.clone(self._inner_str[self._sep_pos+1:]))
            # Now match against each prefix up to every **.
            nxt_stst = self._stst_pos
            stop_pos = self._sep_pos if self._sep_pos > 0 \
                                        else len(self._inner_str)
            while 0 <= nxt_stst < stop_pos:
                if fnmatch.fnmatch(component, self._inner_str[:nxt_stst+1]):
                    tail_str = self._inner_str[nxt_stst:]
                    tails.add(self.clone(tail_str))
                nxt_stst = self._inner_str.find(_STST, nxt_stst+2)
        else:  # No ** and no /
            if fnmatch.fnmatch(component, self._inner_str):
                tails.add(self.clone(fstr("")))
        return tails

    def matches_head(self, component):
        """
        True if basename matches as first component.
        """
        return self.head_to_tails(component)

    def matches_exactly(self, component):
        """
        True if basename matches the full pattern.
        """
        tails = self.head_to_tails(component)
        return any(t.is_empty() for t in tails)

    def matches_path(self, path):
        if path[0:1] == _SEP:
            path = path[1:]
        path = fstr(path)
        patterns = [self]
        for component in path.split(_SEP):
            new_pats = []
            for p in patterns:
                new_pats += p.head_to_tails(component)
            patterns = new_pats
        return any(p.is_empty for p in patterns)

    def is_exclude(self):
        return self._type == "e"

    def is_include(self):
        return self._type == "i"

class ExcludePattern(Pattern):
    __slots__ = ()
    def __init__(self, path_glob):
        super(ExcludePattern, self).__init__(path_glob, pattern_type="e")

class IncludePattern(Pattern):
    __slots__ = ()
    def __init__(self, path_glob):
        super(IncludePattern, self).__init__(path_glob, pattern_type="i")


class GlobMatcher:
    """
    Match relative filenames to a list of glob patterns and create subdir
    matchers in a recursive-friendly way.
    """
    __slots__ = ["_patterns"]

    def __init__(self, patterns=None):
        """
        patterns is a list of (tag, glob pattern string).
        """
        if patterns is None:
            patterns = []
        assert isinstance(patterns, list)
        self._patterns = patterns

    def all_patterns_iter(self):
        return self._patterns

    def exclude_file_bname(self, file_bname):
        """
        Return True if the single basename file_bname matches some exclude
        pattern before matching any include pattern.
        """
        res = False
        for pat in self.all_patterns_iter():
            if pat.is_dir_matcher():
                continue
            if pat.matches_head(file_bname):
                if pat.is_include():
                    res = False
                else:
                    res = True
                break
        return res

    def exclude_dir_bname(self, dir_bname):
        """
        Return True if dir_bname matches some exclude pattern before matching
        any include pattern.
        """
        res = False
        for pat in self.all_patterns_iter():
            if pat.matches_head(dir_bname):
                if pat.is_include():
                    res = False
                else:
                    res = True
                break
        return res

    def to_subdir(self, dir_bname):
        """
        Return a GlobMatcher representing patterns to use one directory down.
        """
        subdir_patterns = [] # Include/Exclude need to be kept in order.
        for pat in self.all_patterns_iter():
            if not pat.is_anchored():
                subdir_patterns.append(pat)
            for tail_pat in pat.head_to_tails(dir_bname):
                if not tail_pat.is_empty():
                    subdir_patterns.append(tail_pat)
        if subdir_patterns:
            new_glob_matcher = GlobMatcher([])
            new_glob_matcher._patterns = subdir_patterns
            res = new_glob_matcher
        else:
            res = None
        return res
