#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
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
Pattern - a glob pattern with the rsync-link ability to match at the root dir
    only (anchoring with / prefix) and to match only dirs (trailing /)/.
    Patterns may be set to exclude or to include.

GlobMatcher - a set of patterns suited to match when traversing a tree
    starting at the root.
"""

# pylint: disable=invalid-name, protected-access, misplaced-comparison-constant

import fnmatch

import lnsync_pkg.printutils as pr

_SEP = "/"
_STST = "**"

class Pattern:
    __slots__ = ("_inner_str", "_type",
                 "_sep_pos", "_stst_pos",
                 "_anchored", "_dir_matcher")
    def __init__(self, glob_string, pattern_type=None):
        """
        pattern_type _type is either "i" or "e", for exclude or include.
        """
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
        new_inner_str = self.to_str(inner_str=new_inner_str)
        new_pat = (type(self))(new_inner_str)
        return new_pat

    def to_str(self, inner_str=None):
        """
        Return the pattern as a string.
        """
        if inner_str is None:
            inner_str = self._inner_str
        prefix = _SEP if self._anchored else ""
        postfix = _SEP if self._dir_matcher else ""
        return prefix + inner_str + postfix

    def __hash__(self):
        return hash(self.to_str())

    def __eq__(self, other):
        return self.to_str() == other.to_str() \
                and self._type == other._type

    def is_anchored(self):
        return self._anchored

    def is_dir_matcher(self):
        return self._dir_matcher

    def is_empty(self):
        """
        Test if we match only the empty string.
        """
        return len(self._inner_str) == 0

    def matches_empty(self):
        """
        Test if we match the empty string.
        """
        return self._sep_pos < 0 and fnmatch.fnmatch("", self._inner_str)

    def head_to_tails(self, component):
        """
        Given a non-empty path component, return a list of new tail patterns
        such that we match <component>/<tail> iff some tail pattern matches
        <tail>.
        """
        tails_pats = set()
        if 0 <= self._sep_pos < self._stst_pos \
                or self._stst_pos < 0 <= self._sep_pos:
            # There is some / before any **.
            if fnmatch.fnmatch(component, self._inner_str[:self._sep_pos]):
                tail_str = self._inner_str[self._sep_pos+1:]
                tails_pats.add(self.clone(tail_str))
        elif 0 <= self._stst_pos:
            # There is some ** before any /.
            # First:
            # Match everything up to either a / or the end, whichever is first.
            stop_pos = self._sep_pos if self._sep_pos > 0 \
                       else len(self._inner_str)
            if fnmatch.fnmatch(component, self._inner_str[:stop_pos]):
                tails_pats.add(self.clone(self._inner_str[stop_pos+1:]))
            # Next, match up to every ** before any /.
            nxt_stst = self._stst_pos
            while 0 <= nxt_stst < stop_pos:
                if fnmatch.fnmatch(component, self._inner_str[:nxt_stst+1]):
                    tail_str = self._inner_str[nxt_stst:]
                    tails_pats.add(self.clone(tail_str))
                nxt_stst = self._inner_str.find(_STST, nxt_stst+2)
        else:  # No ** and no /
            if fnmatch.fnmatch(component, self._inner_str):
                tails_pats.add(self.clone(""))
        return tails_pats

    def matches_exactly(self, component):
        """
        True if basename matches the full pattern.
        """
        tails = self.head_to_tails(component)
        return any(t.matches_empty() for t in tails)

    def matches_path(self, path):
        """
        Match a pattern against a path literal.
        The path is taken to be a directory iff it has a trailing slash.
        """
        if path[0:1] == _SEP:
            path = path[1:]
        if path[-1:] == _SEP:
            path = path[:-1]
            path_is_dir = True
        else:
            path_is_dir = False
        if not path_is_dir and self._dir_matcher:
            return False
        patterns = [self]
        for component in path.split(_SEP):
            new_pats = []
            for p in patterns:
                new_pats += p.head_to_tails(component)
            patterns = new_pats
        return any(p.matches_empty() for p in patterns)

    def is_exclude(self):
        return self._type == "e"

    def is_include(self):
        return self._type == "i"

    def __str__(self):
        return "{%s|%s%s%s}" % (self._type,
                                "/" if self._anchored else "",
                                self._inner_str,
                                "/" if self._dir_matcher else "")

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
        assert isinstance(patterns, list), \
            f"GlobMatcher.__init__: not a list: {patterns}"
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
            if pat.matches_exactly(file_bname):
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
            if pat.matches_exactly(dir_bname):
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

    def __str__(self):
        return "{g|%s}" % (",".join(str(p) for p in self._patterns))

def merge_pattern_lists(pats1, pats2):
    """
    Given two lists of exclude/include patterns, merge them.
    """
    # Make sure the exclusions are kept in sync.
    def prnt_pats(plist):
        def pre(p):
            return "--exclude" if p.is_exclude() else "--include"
        return " ".join("%s %s" % \
            (pre(p), p.to_str()) for p in plist)
    def merge_lists(l1, l2):
        # Input lists may be altered.
        if not l1:
            return l2
        if not l2:
            return l1
        e1, e2 = l1[0], l2[0]
        if e1 == e2:
            return [e1, *merge_lists(l1[1:], l2[1:])]
        elif e1 in l2 and e2 in l1:
            raise ValueError(
                "cannot preserve order of exclusions: %s %s" % \
                (prnt_pats(l1), prnt_pats(l2)))
        elif e1 in l2:
            return [e2, *merge_lists(l1, l2[1:])]
        elif e2 in l1:
            return [e1, *merge_lists(l1[1:], l2)]
        else:
            return [e1, e2, *merge_lists(l1[1:], l2[1:])]
    common_pats = merge_lists(list(pats1), list(pats2))
    if common_pats != pats1 or common_pats != pats1:
        if any(all(all(getattr(p, f)() for p in pset)
                   for pset in (pats1, pats2))
               for f in ("is_include", "is_exclude")):
            outf = pr.info
        else:
            outf = pr.warning
        outf("merged rules [%s] and [%s] to [%s]" % \
            (prnt_pats(pats1), prnt_pats(pats2),
             prnt_pats(common_pats)))
    return common_pats
