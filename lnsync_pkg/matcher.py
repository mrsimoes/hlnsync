#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""Match source and target file trees, based on file size and content hash.

Each source file may correspond to multiple source paths,
and likewise for the target tree.

A target may match at most one source file, with the same .

Given a match, generate a sequence of permissible and reversible
ln/rm/mv operations that, applied to the target FileTree, will bring it
in sync with the source, as much as possible.
"""

from __future__ import print_function

import os
import itertools
import copy
from collections import namedtuple

import lnsync_pkg.printutils as pr
from lnsync_pkg.onegraph import OneGraph
from lnsync_pkg.backtracker import SearchState, do_search

SrcTgt = namedtuple("SrcTgt", ["src", "tgt"])

class TreePairMatcher(object):
    """Match file trees and generate target path sync (mv, ln, rm) commands.

    Target tree must be in online mode.
    If source tree is also online, it cannot be a subdir of the target, else
    ValueError is raised.
    """
    def __init__(self, src_tree, tgt_tree):
        assert tgt_tree.db.mode == "online", "TreePairMatcher: not online db."
        # Do not match a tree against a subtree.
        def is_subdir(path, directory):
            relative = os.path.relpath(path, directory)
            return not relative.startswith(os.pardir + os.sep)
        if src_tree.db.mode == "online" and \
            (is_subdir(src_tree.root_path, tgt_tree.root_path) \
             or is_subdir(tgt_tree.root_path, src_tree.root_path)):
            raise ValueError("Source tree cannot be a subdirectory of target.")
        self.trees = SrcTgt(src_tree, tgt_tree)
        self.mt_state = State(self.trees)
        self._have_match = False
        self._rm_in_advance = None # Used in generating commands.

    def do_match(self):
        """Run matching algorithm, return True if match was found.
        """
        pr.progress("starting match-up")
        initial_state = self.mt_state
        pr.progress("completing match-up")
        have_match = do_search(initial_state)
        pr.progress("finished match-up")
        self._have_match = have_match
        return have_match

    def generate_sync_cmds(self):
        """Yield sync cmds, each a tuple (cmd arg1 arg2).

        arg1 and arg2 are always relative pathnames.
        Possible commands are:
            ("mv", from_path, to_path)
            ("ln", existing_path, new_path),
            ("rm", path_to_rm  alt_file_path)
        """
        assert self._have_match, "generate_sync_cmds: not matched yet"
        # Execute ln commands first, before mv and rm.
        self._rm_in_advance = set()
        return itertools.chain(
            self._gen_ln_cmds(),
            self._gen_mv_cmds(),
            self._gen_rm_cmds())

    def _gen_ln_cmds(self):
        """Generate mkdir/ln commands to create new links.
        """
        ln_map = self.mt_state.total_path_op.ln_map
        for ln_ref_path in ln_map:
            for new_link_path in ln_map[ln_ref_path]:
                if self.trees.tgt.follow_path(new_link_path):
                    if new_link_path in self.mt_state.total_path_op.unln_set:
                        self._rm_in_advance.add(new_link_path)
                        yield self._mk_rm_cmd(new_link_path)
                    else:
                        pr.warning("cannot create hardlink at %s" % \
                            new_link_path)
                        continue
                yield ("ln", ln_ref_path, new_link_path)

    def _gen_mv_cmds(self):
        """Generate mkdir/mv commands.
        """
        mv_graph = self.mt_state.total_path_op.mv_graph
        roots = mv_graph.get_all_roots()
        for mv_graph_root in roots:
            # Follow maximal mv_graph path a1->a2->...->an
            # to get mv an-1 an; ... ; mv a2 a3; mv a1 a2 .
            reversed_rel_mv_pairs = []
            curr_path = mv_graph_root
            while True:
                new_path = mv_graph.follow_arrow(curr_path)
                if new_path is None:
                    break
                reversed_rel_mv_pairs.append((curr_path, new_path))
                curr_path = new_path
            final_mv_pair = reversed_rel_mv_pairs[-1]
            final_mv_dest = final_mv_pair[1]
            if self.trees.tgt.follow_path(final_mv_pair[1]):
                if final_mv_dest in self.mt_state.total_path_op.unln_set:
                    self._rm_in_advance.add(final_mv_dest)
                    yield self._mk_rm_cmd(final_mv_dest)
                else:
                    pr.warning("cannot mv to %s" % final_mv_pair[1])
                    continue
            reversed_rel_mv_pairs.reverse()
            rel_mv_pairs = reversed_rel_mv_pairs
            for curr_path, new_path in rel_mv_pairs:
                yield ("mv", curr_path, new_path)

    def _gen_rm_cmds(self):
        """Generate rm commands.
        """
        for relpath in self.mt_state.total_path_op.unln_set:
            if relpath not in self._rm_in_advance:
                yield self._mk_rm_cmd(relpath)

    def _mk_rm_cmd(self, relpath):
        """Create a rm command.

        A rm command includes an alternative path to the same file as its
        second paramater, to allow undoing.
        """
        f_obj = self.trees.tgt.follow_path(relpath)
        witness_path = f_obj.relpaths[0]
        if witness_path == relpath:
            witness_path = f_obj.relpaths[1]
        return ("rm", relpath, witness_path)

class PathOp(object):
    """A combined mv/ln/rm set of operations to be applied at the target.

    Consists of:
        - mv_graph: a 1-graph where each arc oldpath1->newpath1 corresponds
            to a mv operation.
        - ln_map: a map {current_path -> [new_path_1, ...]} of current
            paths to links to be created
        - unln_set: set {current_path_1, ...} of paths to be unlinked.
    """
    __slots__ = "mv_graph", "ln_map", "unln_set"

    @staticmethod
    def join(*pathops):
        """Create a PathOp by merging pathops."""
        if pathops == ():
            total_op = PathOp.make_trivial()
        else:
            total_op = pathops[0]
            for path_op in pathops[1:]:
                total_op.add(path_op)
        return total_op

    @staticmethod
    def make_mv(st_path):
        """PathOp for mv {tpath->spath} from SrcTgt(spath, tpath).
        Represents 'mv tpath spath' operation.
        """
        if st_path.src == st_path.tgt:
            return PathOp.make_trivial()
        else: # Arrow from tpath to spath.
            mv_g = OneGraph()
            mv_g.add_arrow(st_path.tgt, st_path.src)
            return PathOp(mv_g, {}, set())

    @staticmethod
    def make_unln(*paths):
        """PathOp for unlinking paths."""
        return PathOp(OneGraph(), {}, set(paths))

    @staticmethod
    def make_ln(tgt_path, ln_paths):
        """PathOp for hard linking {tgtpath->[newnames]}.
        """
        return PathOp(OneGraph(), {tgt_path: ln_paths}, set())

    @staticmethod
    def make_trivial():
        return PathOp(OneGraph(), {}, set())

    def __init__(self, mv_graph, ln_map, unln_set):
        self.mv_graph = mv_graph
        self.ln_map = ln_map
        self.unln_set = unln_set

    def add(self, another_op):
        """Merge another_op into self, return self.
        """
        self.mv_graph.add_graph(another_op.mv_graph)
        for tgt_path in another_op.ln_map:
            assert not tgt_path in self.ln_map, \
                "PathOp.add: tgt_path in ln_map."
            self.ln_map[tgt_path] = another_op.ln_map[tgt_path]
        for tgt_path in another_op.unln_set:
            assert not tgt_path in self.unln_set, \
                "PathOp.add: tgt_path in unln_set."
            self.unln_set.add(tgt_path)
        return self

    def remove(self, another_op):
        """Remove another_op elementary operations from self.
        """
        self.mv_graph.remove_graph(another_op.mv_graph)
        for tgt_path in another_op.ln_map:
            assert tgt_path in self.ln_map, \
                "PathOp.rm: tgt_path not in ln_map."
            del self.ln_map[tgt_path]
        for tgt_path in another_op.unln_set:
            assert tgt_path in self.unln_set, \
                "PathOp.rm: tgt_path not in unln_set."
            self.unln_set.remove(tgt_path)

    def is_valid(self):
        return not self.mv_graph.has_cycle()

class State(SearchState):
    """State for matching using the backtracker.

    Implements make_delta_iter, that generates deltas.
    An delta is the data used by a parent node to generate one of the children
    node, and again by that child node to revert back to the same parent node.
    State data:
        - szhash_stack: stack of pairs of file id lists (src_ids, tgt_ids),
        all identical sizes and hashes in each pair, yet to be matched
        - cur_srctgt_ids = SrcTgt(srclist, tgtlist): files yet to be matched
        for the current size-hash values
        - total_path_op: graph of target file operations built so far
    Static data:
        - trees: SrcTgt(src_tree, tgt_tree)
        - szhash_to_ids: full {(size, hash) -> [SrcTgtfile_ids pairs]}
    """

    __slots__ = "szhash_stack", "cur_srctgt_ids", "total_path_op", \
                "trees", "szhash_to_src_ids", "_valid"

    def __init__(self, trees):
        """trees is a SrcTgt pair (src_tree, tgt_tree).

        Init state variables and static data szhash_to_ids.
        """
        self.trees = trees
        self.total_path_op = PathOp.make_trivial()
        self.szhash_stack = []
        self.szhash_to_ids = {}
        self._init_stack_and_pathop()
        self.cur_srctgt_ids = SrcTgt((), ())
        self._doing_final_check = False
        self._valid = True
        self.szhash_cur = None

    def _init_stack_and_pathop(self):
        """Create a stack of (size, hash) to match."""
        def list_intersection(list1, list2):
            tmp = set(list1)
            return [e for e in list2 if e in tmp]
        common_sizes = list_intersection(self.trees.src.get_all_sizes(),
                                         self.trees.tgt.get_all_sizes())
        common_sizes.sort()
        for common_sz in common_sizes:
            with pr.ProgressPrefix("size %d:" % (common_sz,)):
                self._init_stack_and_pathop_persize(common_sz)

    def _init_stack_and_pathop_persize(self, file_sz):
        """Initialize stack, considering only files of a given size,
        eliminating common trivial cases."""
        sz_src_files = self.trees.src.size_to_files(file_sz)
        sz_tgt_files = self.trees.tgt.size_to_files(file_sz)
        src_hashes = {self.trees.src.get_prop(f) for f in sz_src_files}
        tgt_hashes = {self.trees.tgt.get_prop(f) for f in sz_tgt_files}
        sz_common_hashes = set.intersection(src_hashes, tgt_hashes)
        def commonhash_to_fileids(tree, file_list):
            """Return table hash -> [fileids] with all files whose hash is one of
            the values in sz_common_hashes (hashes in common for this file size).
            """
            hashfileids_dict = {}
            for file_obj in file_list:
                hash_val = tree.get_prop(file_obj)
                if hash_val in sz_common_hashes:
                    if hash_val in hashfileids_dict:
                        hashfileids_dict[hash_val].append(file_obj.file_id)
                    else:
                        hashfileids_dict[hash_val] = [file_obj.file_id]
            return hashfileids_dict
        src_commonhash_to_fileids = \
            commonhash_to_fileids(self.trees.src, sz_src_files)
        tgt_commonhash_to_fileids = \
            commonhash_to_fileids(self.trees.tgt, sz_tgt_files)
        for hash_val in sz_common_hashes:
            pr.trace("stack init for (size,hash)=(%d,%d)", file_sz, hash_val)
            szhash_fileids_src = src_commonhash_to_fileids[hash_val]
            szhash_fileids_tgt = tgt_commonhash_to_fileids[hash_val]
            srctgt_ids = SrcTgt(szhash_fileids_src, szhash_fileids_tgt)
            szhash = (file_sz, hash_val)
            if not self._init_eliminated_now(srctgt_ids):
                self.szhash_stack.append(szhash)
                self.szhash_to_ids[szhash] = srctgt_ids

    def _init_eliminated_now(self, srctgt_ids):
        """Return True if these ids were handled now and need not go to the
        search stack."""
# Case 1. one id on each side.
        ids = srctgt_ids
        if len(ids.src) == len(ids.tgt) == 1:
            src_paths = self.trees.src.id_to_file(ids.src[0]).relpaths
            tgt_paths = self.trees.tgt.id_to_file(ids.tgt[0]).relpaths
# Case 1a. single path for each id.
            if len(src_paths) == len(tgt_paths) == 1:
                self.total_path_op.add(
                    PathOp.make_mv(SrcTgt(src_paths[0], tgt_paths[0])))
                return True
# Case 1b. multiple paths for each id, equal in number and as sets.
            elif set(src_paths) == set(tgt_paths):
                return True
# Case 2. same number of ids on each side, each id with a single path,
#         paths match as a set.
        elif len(ids.src) == len(ids.tgt):
            all_src_paths = \
                [self.trees.src.id_to_file(i).relpaths for i in ids.src]
            all_tgt_paths = \
                [self.trees.tgt.id_to_file(i).relpaths for i in ids.tgt]
            if all([len(ps) == 1 for ps in all_src_paths]) \
                    and all([len(ps) == 1 for ps in all_tgt_paths]):
                all_src_paths = [ps[0] for ps in all_src_paths]
                all_tgt_paths = [ps[0] for ps in all_tgt_paths]
                if set(all_src_paths) == set(all_tgt_paths):
                    return True
        return False

    def down_delta(self, state_delta):
        delta = state_delta
        pr.trace("down: %s", delta)
        if delta.next_szhash:
            pr.trace("new sz-hash: %s", delta.next_szhash)
            assert self.szhash_stack[-1] == delta.next_szhash
            self.szhash_cur = self.szhash_stack.pop()
            self.cur_srctgt_ids = \
                copy.deepcopy(self.szhash_to_ids[delta.next_szhash])
        elif delta.skip_ids:
            pr.trace("ignoring leftover ids: %s", delta.skip_ids)
            for s_id in delta.skip_ids.src:
                self.cur_srctgt_ids.src.remove(s_id)
            for t_id in delta.skip_ids.tgt:
                self.cur_srctgt_ids.tgt.remove(t_id)
        elif not delta.final_check:
            pr.trace("new src-tgt id pair: %s", delta.srctgt_id)
            self.cur_srctgt_ids.src.remove(delta.srctgt_id.src)
            self.cur_srctgt_ids.tgt.remove(delta.srctgt_id.tgt)
            self.total_path_op.add(delta.path_op)
            self._valid = self.total_path_op.is_valid()
            pr.trace("new pathop of size %d", len(str(self.total_path_op)))
        else:
            pr.trace("final check.")
            self._doing_final_check = True
            self._valid = True
            pr.trace("valid: %s", self._valid)

    def up_delta(self, state_delta):
        delta = state_delta
        pr.trace("up : %s", delta)
        if delta.next_szhash:
            self.szhash_stack.append(delta.next_szhash)
            self.cur_srctgt_ids = SrcTgt([], [])
        elif delta.skip_ids:
            pr.trace("restoring leftover ids: %s", delta.skip_ids)
            for s_id in delta.skip_ids.src:
                self.cur_srctgt_ids.src.append(s_id)
            for t_id in delta.skip_ids.tgt:
                self.cur_srctgt_ids.tgt.append(t_id)
        elif not delta.final_check:
            self.cur_srctgt_ids.src.append(delta.srctgt_id.src)
            self.cur_srctgt_ids.tgt.append(delta.srctgt_id.tgt)
            self.total_path_op.remove(delta.path_op)
        else:
            self._doing_final_check = False
        self._valid = True

    def is_valid(self):
        return self._valid

    def make_delta_iter(self):
        """Return a non-empty iterator of deltas from current state, or None.
        """
        pr.trace("making delta for stack size %d and cur ids: %s",
                 len(self.szhash_stack), self.cur_srctgt_ids)
        if self.cur_srctgt_ids.src and self.cur_srctgt_ids.tgt:
            src_ids = copy.deepcopy(self.cur_srctgt_ids.src)
            tgt_ids = copy.deepcopy(self.cur_srctgt_ids.tgt)
            def delta_maker():
                for (sids, tids) in self._gen_id_permutations(src_ids, tgt_ids):
                    for sid, tid in zip(sids, tids):
                        for path_op in self._gen_pathops(sid, tid):
                            delta = Delta.make_path_op(
                                SrcTgt(sid, tid), path_op)
                        yield delta
            return delta_maker()
        elif self.cur_srctgt_ids.src or self.cur_srctgt_ids.tgt:
            src_ids = copy.deepcopy(self.cur_srctgt_ids.src)
            tgt_ids = copy.deepcopy(self.cur_srctgt_ids.tgt)
            return iter([Delta.make_skip_ids(SrcTgt(src_ids, tgt_ids))])
        elif self.szhash_stack:
            szhash_delta = Delta.make_next_szhash(self.szhash_stack[-1])
            pr.progress("hash values to go: %d." % (len(self.szhash_stack)))
            return iter((szhash_delta,))
        elif not self._doing_final_check:
            return iter([Delta.make_final_check()])
        else:
            return None

    def _gen_pathops(self, src_id, tgt_id):
        """Generate PathOps turning the tgt_id pathset into the src_id pathset.

        Each generated PathOp accomplishes that goal using different specific
        mv/ln/unln elementary operations, with some possibly creating mv cycles
        when combined with other PathOps for other file ids.
        """
        assert isinstance(src_id, (int, long)) \
            and isinstance(tgt_id, (int, long)), \
            "_gen_pathops: bad ids (%s,%s)." % (src_id, tgt_id)
        src_paths = self.trees.src.id_to_file(src_id).relpaths
        tgt_paths = self.trees.tgt.id_to_file(tgt_id).relpaths
        some_tgt_path = tgt_paths[0]
        common_paths = [p for p in src_paths if p in tgt_paths]
        src_only_paths = [p for p in src_paths if p not in common_paths]
        tgt_only_paths = [p for p in tgt_paths if p not in common_paths]
        if not src_only_paths:
            assert common_paths, "_gen_pathops: no common paths"
            yield PathOp.make_unln(*tgt_only_paths)
        elif not tgt_only_paths:
            assert common_paths, "_gen_pathops: no common paths"
            yield PathOp.make_ln(some_tgt_path, src_paths)
        elif len(tgt_only_paths) == len(src_only_paths):
            # Just mv paths.
            for tgt_paths_order in \
                    itertools.permutations(tgt_only_paths, len(tgt_only_paths)):
                mv_ops = [PathOp.make_mv(SrcTgt(a, b)) \
                            for (a, b) in zip(src_only_paths, tgt_paths_order)]
                yield PathOp.join(*mv_ops)
        elif len(tgt_only_paths) >= len(src_only_paths):
            # Unlink some target paths.
            for tgt_paths_order in \
                    itertools.permutations(tgt_only_paths, len(src_only_paths)):
                mv_ops = [PathOp.make_mv(SrcTgt(a, b)) \
                            for (a, b) in zip(src_only_paths, tgt_paths_order)]
                mv_op = PathOp.join(*mv_ops)
                ln_op = PathOp.make_unln(\
                    *[tn for tn in tgt_only_paths if tn not in tgt_paths_order])
                yield mv_op.add(ln_op)
        else:
            # len(tgt_paths) < len(src_paths) need to create new links at target.
            for src_paths_order \
                    in itertools.permutations(src_paths, len(tgt_paths)):
                mv_ops = [PathOp.make_mv(SrcTgt(a, b)) \
                        for (a, b) in zip(src_paths_order, tgt_paths)]
                mv_op = PathOp.join(*mv_ops)
                ln_op = PathOp.make_ln(
                    some_tgt_path,
                    [sn for sn in src_paths if sn not in src_paths_order])
                yield mv_op.add(ln_op)

    def _gen_id_permutations(self, src_ids, tgt_ids):
        """Match up each src_id to a tgt_id.

        A matchup is a pair of lists (src_ids, tgt_ids) of equal length.
        Return an iterator that will generate matchups.
        In the non-trivial case, the iterator yields first a restricted range of
        matchups, where files sharing a full path are always matched.
        """
        if len(src_ids) == 1 and len(tgt_ids) == 1:
            return iter([(src_ids, tgt_ids)])
        elif len(src_ids) <= len(tgt_ids):
            res = itertools.izip(itertools.repeat(src_ids),\
                                itertools.permutations(tgt_ids, len(src_ids)))
        else:
            res = itertools.izip(itertools.permutations(src_ids, len(tgt_ids)),\
                                    itertools.repeat(tgt_ids))
        bgm = self._best_guess_matches(src_ids, tgt_ids)
        return itertools.chain(bgm, res)

    def _best_guess_matches(self, src_ids, tgt_ids):
        """Return a generator for some matchups of ids1 to ids2.
        See _gen_id_permutations.
        """
        def match_either_way(ids1, tree1, ids2, tree2):
            # Assume len(ids1) <= len(ids2).
            unmt_ids_1 = copy.copy(ids1)
            unmt_ids_2 = copy.copy(ids2)
            mt_ids_1, mt_ids_2 = [], []
            for id1 in ids1:
                id1_matched = False
                paths1 = tree1.id_to_file(id1).relpaths
                for path1 in paths1:
                    for id2 in unmt_ids_2:
                        paths2 = tree2.id_to_file(id2).relpaths
                        if path1 in paths2:
                            mt_ids_1 += [id1]
                            mt_ids_2 += [id2]
                            unmt_ids_1.remove(id1)
                            unmt_ids_2.remove(id2)
                            id1_matched = True
                            break
                    if id1_matched:
                        break
            if mt_ids_1 == []:
                # No id in ids1 has a path in common with some path in ids2.
                return iter([])
            elif len(mt_ids_1) == len(ids1):
                # Each id in ids1 has a path matching a path
                # for some id2 in ids2.
                return iter([(mt_ids_1, mt_ids_2)])
            else:
                return itertools.izip(\
                            itertools.repeat(mt_ids_1 + unmt_ids_1),\
                            itertools.imap(
                                lambda permtail: mt_ids_2 + list(permtail),
                                itertools.permutations(
                                    unmt_ids_2, len(unmt_ids_1))))
        if len(src_ids) == 1 and len(tgt_ids) == 1:
            res = iter([]) # Nothing to add
        elif len(src_ids) <= len(tgt_ids):
            res = match_either_way(
                src_ids, self.trees.src, tgt_ids, self.trees.tgt
                )
        else:
            def reverse_pair(pair):
                return (pair[1], pair[0])
            res = itertools.imap(
                reverse_pair,
                match_either_way(
                    tgt_ids, self.trees.tgt, src_ids, self.trees.src)
                )
        return res

class Delta(object):
    """State delta for passing to a child node and back.

    There are four types of Deltas: a Delta has fields new_szhash, srctgt_id,
    path_op, skip_ids, final_check, interpreted as follows (going down the
    search tree to a child node).
    1.If new_szhash is not None, then it is the value at the top of szhash
    stack.
    Action: new_szhash is popped from szhash_stack and cur_srctgt_ids is set.
    2.If srctgt_id is not None, then srctgt_id.src is in cur_srctgt_ids.src and
    likewise for tgt. Action: srctgt_id.src removed cur_srctgt_ids.src
    (likewise for tgt) and path_op for this match is merged into total_path_op.
    3.If skip_ids is not None, it is a SrcTgt pair of lists that were not
    matched and are to be disregarded. Action: clear srctgt_id.
    4.If final_check is not None, then the search stack is empty.
    Action: the state is flagged as being checked.
    """

    __slots__ = "next_szhash", "srctgt_id", "path_op", "skip_ids", "final_check"

    @staticmethod
    def make_next_szhash(next_szhash):
        """Return a Delta for passing to the next size/hash pair on the stack.
        """
        newobj = Delta()
        newobj.next_szhash = next_szhash
        return newobj

    @staticmethod
    def make_path_op(srctgt_id, path_op):
        """Return a Delta for adding a srcid/tgtid match to the state."""

        newobj = Delta()
        newobj.srctgt_id = srctgt_id
        newobj.path_op = path_op
        return newobj

    @staticmethod
    def make_skip_ids(srctgt_ids):
        """Return a Delta for skipping srcids/tgtids."""
        newobj = Delta()
        newobj.skip_ids = srctgt_ids
        return newobj

    @staticmethod
    def make_final_check():
        """Return a final check Delta."""

        newobj = Delta()
        newobj.final_check = True
        return newobj

    def __init__(self):
        self.next_szhash = None
        self.srctgt_id = None
        self.path_op = None
        self.skip_ids = None
        self.final_check = False

    def __str__(self):
        if self.next_szhash:
            return "->szhash: %s" % (self.next_szhash,)
        elif self.skip_ids:
            return "-> skip ids: %s" % (self.skip_ids,)
        elif not self.final_check:
            return "->%s %s" % (self.srctgt_id, self.path_op)
        else:
            return "->final_check"

    def __repr__(self):
        return self.__str__()
