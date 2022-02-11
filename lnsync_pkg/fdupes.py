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

from lnsync_pkg.proptree import FileItem, TreeError
import lnsync_pkg.printutils as pr

def _get_prop(tree, fobj):
    """
    Return prop if possible, or None otherwise.
    """
    assert isinstance(fobj, FileItem), \
        "getprop: not a FileItem"
    try:
        prop = tree.get_prop(fobj)
    except TreeError as exc:
        pr.error(
            "processing file id %d, ignored: %s" % (fobj.file_id, str(exc)))
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
        this_tree_sizes = list(tree.get_all_sizes())
        this_tree_sizes.sort()
        for file_sz in this_tree_sizes:
            if file_sz in sizes_seen_twice:
                continue
            elif file_sz in sizes_seen_once:
                sizes_seen_once.remove(file_sz)
                sizes_seen_twice.add(file_sz)
                yield file_sz
            else:
                files_this_sz = tree.size_to_files(file_sz)
                if len(files_this_sz) > 1 or \
                        (not hard_links and len(files_this_sz[0].relpaths) > 1):
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

def sizes_onall(all_trees):
    """
    Yield all sizes for which at least one file of that size exists on
    each of all_trees.
    """
    if len(all_trees) >= 1:
        pr.progress("scanning sizes")
        trees_sizescounts = \
            [(tree, len(tree.get_all_sizes())) for tree in all_trees]
        pr.progress("sorting sizes")
        trees_sizescounts.sort(key=lambda ts: ts[1])
        first_tree = trees_sizescounts[0][0]
        other_trees = [ts[0] for ts in trees_sizescounts[1:]]
        candidate_sizes = list(first_tree.get_all_sizes())
        candidate_sizes.sort()
        for file_sz in candidate_sizes:
            good_size = True
            for tree in other_trees:
                if not tree.size_to_files(file_sz):
                    good_size = False
                    break
            if good_size:
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
    Yield prop=None if size is specified and there is a single file of that size.
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
    If no size is given:
        Consider all files, which doesn't require pre-scanning the tree.
    """
    def _props_onfirstnotonly_of_size(trees, file_sz):
        """
        Considering only files of the given size, yield all props with at
        least one file in the first tree and some file on some of the remaining
        trees.
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
        first_tree = all_trees[0]
        assert first_tree.size_to_files(file_sz), \
            "located_files_onfirstonly_of_size: expected files"
        # If there is a single file of that size in the first tree,
        # no need to compute the property value. TODO
        for prop in _props_onfirstnotonly_of_size(all_trees, file_sz):
            yield prop, \
                  _located_files_by_prop_of_size(
                      all_trees, prop, file_sz)
