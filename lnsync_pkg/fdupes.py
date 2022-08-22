#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Algorithms for finding sizes for which there are is more than one file in a list
of FilePropDB, sizes for which there is one file in each of the FilePropDB in a
list etc, and also algorithms for listing the corresponding files and file
paths.

All that is used from the FilePropTree interface is:
    size_to_files: FilePropTree x int ->  list of FileItem
    size_to_files_gen: FilePropTree x int ->  generator of FileItem
    get_prop: tree x fobj -> int
And for FileItem:
    file_id : FileItem -> int
    relpaths : FileItem -> list of string
"""

from collections import defaultdict

from lnsync_pkg.miscutils import iter_is_empty, iter_len
from lnsync_pkg.fileproptree import FileItem, TreeNoPropValueError
import lnsync_pkg.printutils as pr

def _get_prop(tree, fobj):
    """
    Return prop if possible, or None otherwise.
    """
    assert isinstance(fobj, FileItem), \
        "getprop: not a FileItem"
    try:
        prop = tree.get_prop(fobj)
    except TreeNoPropValueError as exc:
        if exc.first_try:
            pr.error(f"processing, ignored: {str(exc)}")
        return None
    else:
        return prop

def sizes_repeated(all_trees, hard_links):
    """
    Yield all file sizes for which more than two or more files of that
    size exist somewhere across all dbs.
    If hard_links is False, consider different paths to the same file as
    distinct for this purpose.
    """
    sizes_seen_once, sizes_seen_twice = set(), set()
    for tree in all_trees:
        this_tree_sizes = sorted(tree.get_possible_sizes())
        for file_sz in this_tree_sizes:
            if file_sz in sizes_seen_twice:
                continue
            elif file_sz in sizes_seen_once:
                sizes_seen_once.remove(file_sz)
                sizes_seen_twice.add(file_sz)
                yield file_sz
            else:
                files_this_sz = tree.size_to_files_gen(file_sz)
                try:
                    first_file = next(files_this_sz)
                except StopIteration:
                    continue
                try:
                    second_file = next(files_this_sz)
                except StopIteration:
                    second_file = None
                if second_file is not None or \
                        (not hard_links and len(first_file.relpaths) > 1):
                # If not hard_links, a size value seen once for an id
                # with multiple paths is recorded as a dupe.
                    sizes_seen_twice.add(file_sz)
                    yield file_sz
                else:
                    sizes_seen_once.add(file_sz)

def located_files_repeated_of_size(all_trees, file_sz, hard_links):
    """
    Yield all tuples (prop, {tree1: [files_1],... {tree_k, [files_k]})
    over all file props which correspond to more than one file across all trees.
    If file_sz is None, go over all files.
    If hard_links is False, count multiple paths to the same file as repeats of
    the prop.
    """
    # For size sz and all trees, these are {prop: {tree: [fobjs]}}
    props_once_tree_fobjs = defaultdict(lambda: defaultdict(lambda: []))
    props_twice_tree_fobjs = defaultdict(lambda: defaultdict(lambda: []))
    for tree in all_trees:
        for fobj in tree.size_to_files_gen(file_sz):
            prop_val = _get_prop(tree, fobj)
            if prop_val is None:
                continue
            if prop_val in props_twice_tree_fobjs:
                props_twice_tree_fobjs[prop_val][tree].append(fobj)
            elif prop_val in props_once_tree_fobjs:
                props_twice_tree_fobjs[prop_val] = \
                    props_once_tree_fobjs[prop_val]
                del props_once_tree_fobjs[prop_val]
                props_twice_tree_fobjs[prop_val][tree].append(fobj)
            else:
                if not hard_links and len(fobj.relpaths) > 1:
                    props_twice_tree_fobjs[prop_val][tree] = [fobj]
                else:
                    props_once_tree_fobjs[prop_val][tree] = [fobj]
    for prop_val in props_twice_tree_fobjs:
        yield prop_val, props_twice_tree_fobjs[prop_val]

def located_files_on_more_than_one_tree(all_trees, file_sz, hard_links):
    """
    Yield all tuples (prop, {tree1: [files_1],... {tree_k, [files_k]})
    over all file props which correspond to a file on at least two trees.
    If file_sz is None, go over all files.
    If hard_links is False, count multiple paths to the same file as repeats of
    the prop.
    """
    # For size sz and all trees, these are {prop: {tree: [fobjs]}}
    props_one_tree_fobjs = defaultdict(lambda: defaultdict(lambda: []))
    props_two_tree_fobjs = defaultdict(lambda: defaultdict(lambda: []))
    for tree in all_trees:
        props_new_this_tree = defaultdict(lambda: [])
        for fobj in tree.size_to_files_gen(file_sz):
            prop_val = _get_prop(tree, fobj)
            if prop_val is None:
                continue
            if prop_val in props_two_tree_fobjs:
                props_two_tree_fobjs[prop_val][tree].append(fobj)
            elif prop_val in props_one_tree_fobjs:
                props_two_tree_fobjs[prop_val] = \
                    props_one_tree_fobjs[prop_val]
                del props_one_tree_fobjs[prop_val]
                props_two_tree_fobjs[prop_val][tree].append(fobj)
            else:
                if not hard_links and len(fobj.relpaths) > 1:
                    props_two_tree_fobjs[prop_val][tree] = [fobj]
                else:
                    props_new_this_tree[prop_val] = [fobj]
        for prop, fobjs in props_new_this_tree.items():
            props_one_tree_fobjs[prop][tree] = fobjs
    for prop_val in props_two_tree_fobjs:
        yield prop_val, props_two_tree_fobjs[prop_val]

def sizes_onall(all_trees):
    """
    Yield all sizes for which at least one file of that size exists on
    each of all_trees.
    """
    if len(all_trees) >= 1:
        pr.progress("scanning sizes")
        least_sizes_tree = min(all_trees, key=lambda t: iter_len(t.get_possible_sizes()))
        candidate_sizes = sorted(least_sizes_tree.get_possible_sizes())
        for file_sz in candidate_sizes:
            if all(not iter_is_empty(tr.size_to_files_gen(file_sz)) \
                   for tr in all_trees): # Check also first tree.
                yield file_sz

def _located_files_by_prop_of_size(trees, prop, file_sz):
    """
    Return located files of size file_sz matching property prop.
    If file_sz is None, search over all files.
    """
    located_files = defaultdict(lambda: [])
    for tree in trees:
        for fobj in tree.size_to_files_gen(file_sz):
            this_prop = _get_prop(tree, fobj)
            if this_prop is not None and this_prop == prop:
                located_files[tree].append(fobj)
    return located_files

def located_files_onall_of_size(all_trees, file_sz):
    """
    Yield all tuples (prop, {tree1: [files_1],... {tree_k, [files_k]})
    over all file props for which there is a corresponding file in each of the
    trees.
    if file_sz is None, go over all files.
    """
    def _props_onall_of_size(trees, file_sz):
        """
        Considering only files of the given size, yield all
        props with at least one file in each trees.
        """
        good_props = set()
        first_tree = trees[0]
        pr.progress("scanning:", first_tree.printable_path())
        for fobj in first_tree.size_to_files_gen(file_sz):
            prop = _get_prop(first_tree, fobj)
            if prop is None:
                continue
            good_props.add(prop)
        for tree in trees[1:]:
            pr.progress("scanning:", tree.printable_path())
            this_tree_props = set()
            for fobj in tree.size_to_files_gen(file_sz):
                prop = _get_prop(tree, fobj)
                if prop is None:
                    continue
                this_tree_props.add(prop)
            good_props.intersection_update(this_tree_props)
        yield from good_props
    if len(all_trees) >= 1:
        for prop in _props_onall_of_size(all_trees, file_sz):
            yield prop, _located_files_by_prop_of_size(all_trees, prop, file_sz)

def located_files_onfirstonly_of_size(all_trees, file_sz):
    """
    Yield all tuples (prop, {tree_1: [files_1],... {tree_k, [files_k]})
    over all file props for which there is at least one file in the first tree
    and no files in any other trees.
    Assume there is some file of that size on the first tree.
    If file_sz is None, go over all files.
    Yield prop=None if size is specified and there's a single file of that size.
    """
    def _props_onfirstonly_of_size(trees, file_sz):
        """
        Considering only files of the given size, yield all props with at
        least one file the first tree and no other files on any of the remaining
        trees.
        If file_sz is None, consider all files.
        """
        good_props = set()
        first_tree = trees[0]
        pr.progress("scanning:", first_tree.printable_path())
        for fobj in first_tree.size_to_files_gen(file_sz):
            prop = _get_prop(first_tree, fobj)
            if prop is None:
                continue
            good_props.add(prop)
        for tree in trees[1:]:
            pr.progress("scanning:", tree.printable_path())
            if not good_props:
                break
            for fobj in tree.size_to_files_gen(file_sz):
                prop = _get_prop(tree, fobj)
                if prop in good_props:
                    good_props.remove(prop)
        yield from good_props

    if len(all_trees) >= 1:
        first_tree = all_trees[0]
        assert (file_sz is None) or first_tree.size_to_files(file_sz), \
            "located_files_onfirstonly_of_size: expected files"
        # If there is a single file of that size in the first tree,
        # no need to compute the property value.
        if file_sz is not None:
            unique_file = (len(first_tree.size_to_files(file_sz)) == 1)
            if unique_file:
                for tree in all_trees[1:]:
                    if tree.size_to_files(file_sz):
                        unique_file = False
                        break
            if unique_file:
                yield None, {first_tree: first_tree.size_to_files(file_sz)}
                return
        for prop in _props_onfirstonly_of_size(all_trees, file_sz):
            yield prop, \
                  _located_files_by_prop_of_size(
                      all_trees[0:1], prop, file_sz)


def located_files_onfirstnotonly_of_size(all_trees, file_sz=None):
    """
    Yield all tuples (prop, {tree_1: [files_1],... {tree_k, [files_k]})
    over all file props for which there is at least one file in the first tree
    and some file in any of the other trees.
    If a size is given:
        Assume there is some file of that size on the first tree.
    If file_sz is None, go over all files.
        Consider all files, which doesn't require pre-scanning the tree.
    """
    def _props_onfirstnotonly_of_size(trees, file_sz):
        """
        Considering only files of the given size, yield all props with at
        least one file in the first tree and some file on some of the remaining
        trees.
        If file_sz is None, consider all files.
        """
        candidate_props = set()
        good_props = set()
        first_tree = trees[0]
        pr.progress("scanning:", first_tree.printable_path())
        for fobj in first_tree.size_to_files_gen(file_sz):
            prop = _get_prop(first_tree, fobj)
            if prop is None:
                continue
            candidate_props.add(prop)
        for tree in trees[1:]:
            if not candidate_props:
                break
            pr.progress("scanning:", tree.printable_path())
            for fobj in tree.size_to_files_gen(file_sz):
                prop = _get_prop(tree, fobj)
                if prop in candidate_props:
                    candidate_props.remove(prop)
                    good_props.add(prop)
        yield from good_props

    if len(all_trees) >= 2:
        for prop in _props_onfirstnotonly_of_size(all_trees, file_sz):
            yield prop, \
                  _located_files_by_prop_of_size(
                      all_trees, prop, file_sz)
