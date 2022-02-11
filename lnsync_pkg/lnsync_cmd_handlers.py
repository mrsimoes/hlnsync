#!/usr/bin/python3

# Copyright (C) 2020 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Command handlers do_sync, do_rsync, do_search, do_cmp, do_check
"""

# pylint: disable=too-many-nested-blocks, too-many-statements

import os
import subprocess
import shlex
import shutil
import tempfile

from lnsync_pkg.sqlpropdb import SQLPropDBManager
import lnsync_pkg.printutils as pr
from lnsync_pkg.miscutils import is_subdir, HelperAppError
from lnsync_pkg.human2bytes import bytes2human
import lnsync_pkg.fdupes as fdupes
from lnsync_pkg.prefixdbname import pick_db_basename, get_default_dbprefix
from lnsync_pkg.fileid import make_id_computer
from lnsync_pkg.groupedfileprinter import GroupedFileListPrinter
from lnsync_pkg.matcher import TreePairMatcher
from lnsync_pkg.hashtree import \
    FileHashTree, TreeError, PropDBException
from lnsync_pkg.lnsync_treeargs import TreeLocation, TreeLocationOnline
from lnsync_pkg.modaltype import Mode
from lnsync_pkg.hasher_functions import HasherManager

def hash_depends_on_file_size():
    return HasherManager.get_hasher().hash_depends_on_file_size()

def do_rehash(location_arg, relpaths):
    error_found = False

    def rehash_one_file(tree, pr_path, file_obj):
        nonlocal error_found
        try:
            tree.recompute_prop(file_obj)
        except PropDBException as exc:
 # TODO when rehashing a dir, be more specific
 #      find a suitable path of file_obj, under our dir.
            pr.error(f"rehashing {pr_path}: {exc}")
            error_found = True

    with FileHashTree(**location_arg.kws()) as tree:
        for relpath in relpaths:
            tree_obj = tree.path_to_obj(relpath)
            pr_path = tree.printable_path(relpath, pprint=shlex.quote)
            if tree_obj is None:
                pr.error(f"not found:{pr_path}")
                error_found = True
            elif tree_obj.is_file():
                rehash_one_file(tree, pr_path, tree_obj)
            elif tree_obj.is_dir():
                for file_obj in tree.walk_files(tree_obj):
                    rehash_one_file(tree, pr_path, file_obj)
            else:
                pr.error(f"not a file or dir:{pr_path}")
                error_found = True

    return 1 if error_found else 0

def do_lookup(args):
    location_arg = args.location
    relpaths = args.relpaths
    error_found = False
    with FileHashTree(**location_arg.kws()) as tree:
        for relpath in relpaths:
            file_obj = tree.path_to_obj(relpath)
            fname = tree.printable_path(relpath, pprint=shlex.quote)
            if file_obj is None:
                pr.error(f"not found:{fname}")
                error_found = True
                continue
            elif file_obj.is_file():
                # getprop Raises TreeError if no prop.
                prop = tree.get_prop(file_obj)
                pr.print(prop, fname)
            else:
                pr.error(f"not a file:{fname}")
                error_found = True
    return 1 if error_found else 0

def do_sync(args):
    TreeLocation.merge_patterns(args.source, args.target)
    with FileHashTree(**args.source.kws()) as src_tree:
        with FileHashTree(**args.target.kws()) as tgt_tree:
            FileHashTree.scan_trees_async([src_tree, tgt_tree])
            pr.progress("matching...")
            matcher = TreePairMatcher(src_tree, tgt_tree)
            if not matcher.do_match():
                raise NotImplementedError("match failed")
            tgt_tree.writeback = not args.dry_run
            for cmd in matcher.generate_sync_cmds():
                cmd_str = \
                    cmd[0] + " " + " ".join(shlex.quote(arg) for arg in cmd[1:])
                pr.print(cmd_str)
                try:
                    tgt_tree.exec_cmd(cmd)
                except OSError as exc:
                    # E.g. if no linking support on target.
                    raise RuntimeError(f"could not execute: {cmd_str}") from exc
            pr.progress("syncing empty dirs")
            dirs_to_rm_set = set()
            dirs_to_rm_list = []
            for dir_obj, _parent_obj, relpath \
                    in tgt_tree.walk_paths(
                            recurse=True, topdown=False,
                            dirs=True, files=False):
                if all((obj.is_dir() and obj in dirs_to_rm_set) \
                       for obj in dir_obj.entries.values()):
                    if src_tree.path_to_obj(relpath) is None:
                        dirs_to_rm_set.add(dir_obj)
                        dirs_to_rm_list.append(dir_obj)
            err_dir_relpaths = set()
            for dobj in dirs_to_rm_list:
                relpath = dobj.get_relpath()
                pr.print("rmdir " + shlex.quote(relpath))
                # Next follows tgt_tree.writeback.
                try:
                    tgt_tree.rm_dir_writeback(dobj)
                except OSError as exc:
                    if not any(is_subdir(err_dir, relpath) \
                               for err_dir in err_dir_relpaths):
                        pr.info(str(exc))
                        err_dir_relpaths.add(dobj)
            pr.debug("sync done")

def do_rsync(args, rsync_args):
    """
    Print (and optionally execute) a suitable rsync command.
    args.source and args.target are TreeLocation objects,
    with gathered command-line options in the namespace attribute.
    """
    src_dir = args.source.real_location
    tgt_dir = args.target.real_location
    if src_dir[-1] != os.sep:
        src_dir += os.sep # rsync needs trailing / on sourcedir.

# Use subprocess.run with shell=True to display the generated rsync command.
    src_dir = shlex.quote(src_dir)
    tgt_dir = shlex.quote(tgt_dir)
    while tgt_dir[-1] == os.sep:
        tgt_dir = tgt_dir[:-1]

    # Options for rsync: recursive.
    rsync_opts = ["-r", "--size-only", "--progress"]
    if args.hard_links:
        rsync_opts.append("-H")  # Preserve hard links.
    if args.maxsize >= 0:
        rsync_opts.append("--max-size=%d" % args.maxsize)
    if args.dry_run:
        rsync_opts.append("-n")

    # Exclude databases at both ends.
    TreeLocation.merge_patterns(args.source, args.target)
    for pat in getattr(args.source.namespace, "exclude_patterns", []):
        cmd_pattern = pat.to_str()
        vals = ("exclude" if pat.is_exclude() else "include",
                shlex.quote(cmd_pattern))
        rsync_opts.append('--%s=%s' % vals)

    rsync_cmd = ["rsync"] + rsync_opts + rsync_args + [src_dir, tgt_dir]
    rsync_cmd = " ".join(rsync_cmd)
    pr.print(rsync_cmd)

    if args.execute:
        try:
            subprocess.run(rsync_cmd, check=True, shell=True)
        except subprocess.SubprocessError as exc:
            raise HelperAppError(rsync_cmd, str(exc)) from exc

def do_search(args):
    """
    Search for files by relative pattern glob pattern.
    """

    return_code = 1 # If no files are found, return 1.

    def print_file_match(tree, fobj):
        nonlocal return_code
        pr.print(tree.printable_path(files_paths_matched[fobj][0]))
        for fpath in files_paths_matched[fobj][1:]:
            pr.print(" " + tree.printable_path(fpath))
        if args.all_links:
            for fpath in fobj.relpaths:
                if fpath not in files_paths_matched[fobj]:
                    pr.print(" " + tree.printable_path(fpath))

    def search_dir(tree, dir_obj, patterns):
        nonlocal files_paths_to_check
        nonlocal files_paths_matched
        nonlocal return_code
        if not patterns:
            return
        tree.scan_dir(dir_obj)
        patterns = set(patterns)

        def handle_file_match(obj, basename):
            path = os.path.join(dir_obj.get_relpath(), basename)
            if not args.hard_links or len(obj.relpaths) == 1:
                pr.print(tree.printable_path(path))
            else:
                if obj not in files_paths_to_check:
                    files_paths_to_check[obj] = list(obj.relpaths)
                    files_paths_matched[obj] = []
                assert path in files_paths_to_check[obj], \
                    "handle_file_match: path not in paths to check"
                files_paths_to_check[obj].remove(path)
                files_paths_matched[obj].append(path)
                if not files_paths_to_check[obj]:
                    print_file_match(tree, obj)
                    del files_paths_to_check[obj]
                    del files_paths_matched[obj]

        for basename, obj in dir_obj.entries.items():
            if obj.is_file():
                for pat in patterns:
                    if pat.matches_exactly(basename):
                        return_code = 0
                        handle_file_match(obj, basename)
                    break
            if obj.is_dir():
                subdir_patterns = \
                    [p for p in patterns if not p.is_anchored()]
                for pat in patterns:
                    for tail_pat in pat.head_to_tails(basename):
                        if not tail_pat.is_empty():
                            subdir_patterns.append(tail_pat)
                if subdir_patterns:
                    search_dir(tree, obj, subdir_patterns)

    tree_kws = (treearg.kws() for treearg in args.locations)
    with FileHashTree.listof(tree_kws) as all_trees:
        for tree in all_trees:
            if args.hard_links:
                files_paths_to_check = {}
                files_paths_matched = {}
                tree.scan_subtree()
            search_dir(tree, tree.rootdir_obj, [args.glob])
            if args.hard_links:
                for fobj in files_paths_matched:
                    print_file_match(tree, fobj)
    return return_code


def do_cmp(args):
    """
    Recursively compare files and dirs in two directories.
    """

    return_code = 0

    TreeLocation.merge_patterns(args.leftlocation, args.rightlocation)
    def cmp_files(path, left_obj, right_obj):
        nonlocal return_code
        left_prop, right_prop = None, None
        if left_obj.file_metadata.size != right_obj.file_metadata.size:
            pr.print("files differ in size: " + path)
            return_code = 1
            return
        try:
            left_prop = left_tree.get_prop(left_obj)
            right_prop = right_tree.get_prop(right_obj)
        except TreeError:
            if left_prop is None:
                err_path = \
                    left_tree.printable_path(path, pprint=shlex.quote)
            else:
                err_path = \
                    right_tree.printable_path(path, pprint=shlex.quote)
            pr.error(f"reading {err_path}, ignoring")
            return_code = 1
        else:
            if left_prop != right_prop:
                pr.print("files differ in content: " + path)
                return_code = 1
            else:
                if not args.hard_links or \
                    (len(left_obj.relpaths) \
                        == len(right_obj.relpaths) == 1):
                    pr.debug("files equal: %s", path)
                else:
                    left_links = list(left_obj.relpaths)
                    right_links = list(right_obj.relpaths)
                    for left_link in left_obj.relpaths:
                        if left_link in right_links:
                            left_links.remove(left_link)
                            right_links.remove(left_link)
                    if not left_links and not right_links:
                        pr.debug("files equal: %s", path)
                    else:
                        pr.print("files equal, link mismatch:", path)
                        for lnk in left_links:
                            pr.print(" left only link:", lnk)
                        for lnk in right_links:
                            pr.print(" right only link:", lnk)
                        return_code = 1

    def cmp_subdir(dirpaths_to_visit, cur_dirpath):
        for left_obj, basename in \
                left_tree.walk_dir_contents(cur_dirpath, dirs=True):
            left_path = os.path.join(cur_dirpath, basename)
            right_obj = right_tree.path_to_obj(left_path)
            if right_obj is None or right_obj.is_excluded():
                if left_obj.is_file():
                    pr.print("left only: " + left_path)
                elif left_obj.is_dir():
                    pr.print("left only: " + left_path+os.path.sep)
                else:
                    raise RuntimeError("unexpected left object: " + left_path)
            elif left_obj.is_file():
                if  right_obj.is_file():
                    cmp_files(left_path, left_obj, right_obj)
                elif right_obj.is_dir():
                    pr.print("left file vs right dir: " + left_path)
                else:
                    pr.print("left file vs other: " + left_path)
            elif left_obj.is_dir():
                if right_obj.is_dir():
                    dirpaths_to_visit.append(left_path)
                elif right_obj.is_file():
                    pr.print("left dir vs right file: " + left_path)
                else:
                    pr.print("left dir vs other: " + left_path + os.path.sep)
            else:
                raise RuntimeError("unexpected left object: " + left_path)
        for right_obj, basename in \
                right_tree.walk_dir_contents(cur_dirpath, dirs=True):
            right_path = os.path.join(cur_dirpath, basename)
            left_obj = left_tree.path_to_obj(right_path)
            if left_obj is None or left_obj.is_excluded():
                if right_obj.is_file():
                    pr.print("right only: " + right_path)
                elif right_obj.is_dir():
                    pr.print("right only: " + right_path+os.path.sep)
                else:
                    raise RuntimeError(
                        "unexpected right object: " + right_path)
            elif right_obj.is_file():
                if not left_obj.is_file() and not left_obj.is_dir():
                    pr.print("left other vs right file: " + right_path)
            elif right_obj.is_dir():
                if not left_obj.is_file() and not left_obj.is_dir():
                    pr.print("left other vs right dir: " + right_path)
            else:
                raise RuntimeError("unexpected right object: {right_path}")

    with FileHashTree(**args.leftlocation.kws()) as left_tree:
        with FileHashTree(**args.rightlocation.kws()) as right_tree:
            if args.hard_links:
                FileHashTree.scan_trees_async([left_tree, right_tree])
            dirpaths_to_visit = [""]
            while dirpaths_to_visit:
                cur_dirpath = dirpaths_to_visit.pop()
                cmp_subdir(dirpaths_to_visit, cur_dirpath)
    return return_code

def do_check(args):
    def gen_all_paths(tree):
        for _obj, _parent, path in \
                tree.walk_paths(files=True, dirs=False, recurse=True):
            yield path
    with FileHashTree(**args.location.kws()) as tree:
        assert tree.db.mode == Mode.ONLINE, \
            "do_check tree not online"
        if not args.relpaths:
            num_items = tree.get_file_count()
            items_are_paths = False
            paths_gen = gen_all_paths(tree)
        else: # We're iterating over file objects in the tree, not paths.
            num_items = len(args.relpaths)
            items_are_paths = True
            paths_gen = args.relpaths

        def print_report():
            """
            Print report and return final error status.
            """
            pr.print("%d distinct file(s) checked" % \
                     (len(file_objs_checked_ok) \
                      + len(file_objs_checked_bad),))
            if files_skipped > 0:
                pr.print("%d file(s) skipped due to no existing hash" %
                         (files_skipped,))
            if files_error > 0:
                pr.print("%d file(s) skipped due to file error" %
                         (files_error,))
            if file_objs_checked_bad:
                pr.print("%d file(s) failed" % (len(file_objs_checked_bad),))
                for fobj in file_objs_checked_bad:
                    pr.print(tree.printable_path(fobj.relpaths[0]))
                    if args.all_links or not args.hard_links:
                        for other_path in fobj.relpaths[1:]:
                            prefix = "" if not args.hard_links else " "
                            pr.print(prefix, tree.printable_path(other_path))
                res = 1
            else:
                pr.info("no files failed check")
                res = 0
            return res

        def check_one_file(fobj, path):
            if tree.db_check_prop(fobj):
                pr.info(
                    "passed check: " + tree.printable_path(path))
                file_objs_checked_ok.add(fobj)
            else:
                pr.print(
                    "failed check: " + tree.printable_path(path))
                file_objs_checked_bad.add(fobj)

        file_objs_checked_ok = set()
        file_objs_checked_bad = set()
        files_skipped = 0
        files_error = 0
        try:
            index = 1
            for path in paths_gen:
                fobj = tree.path_to_obj(path)
                if fobj in file_objs_checked_ok \
                   or fobj in file_objs_checked_bad:
                    if items_are_paths:
                        index += 1
                    continue
                with pr.ProgressPrefix("%d/%d:" % (index, num_items)):
                    try:
                        check_one_file(fobj, path)
                    except PropDBException as exc:
                        pr.warning(f"not checked: {path} ({exc})")
                        files_skipped += 1
                        continue
                    except TreeError as exc:
                        pr.warning(f"while checking {path}: {exc}")
                        files_error += 1
                        continue
                    index += 1
        finally:
            res = print_report()
    return res

def do_fdupes(args):
    """
    Find duplicate files, using file size as well as file hash.
    """
    return_code = 1 # Default if no duplicates are found.

    grouper = \
        GroupedFileListPrinter(args.hard_links, args.all_links,
                               args.sameline, args.sort)
    with FileHashTree.listof(targ.kws() for targ in args.locations) \
            as all_trees:
        FileHashTree.scan_trees_async(all_trees)
        if hash_depends_on_file_size():
            for file_sz in fdupes.sizes_repeated(all_trees, args.hard_links):
                with pr.ProgressPrefix("size %s:" % (bytes2human(file_sz),)):
                    for _hash, located_files in \
                            fdupes.located_files_repeated_of_size(
                                all_trees, file_sz, args.hard_links):
                        return_code = 0
                        grouper.add_group(located_files)
        else:
            for _hash, located_files in \
                    fdupes.located_files_repeated_of_size(
                        all_trees, None, args.hard_links):
                return_code = 0
                grouper.add_group(located_files)
        grouper.flush()
    return return_code

def do_onall(args):
    return_code = 1 # Default if none are found.
    if len(args.locations) == 1:
        return do_onfirstonly(args)
    grouper = \
        GroupedFileListPrinter(args.hard_links, args.all_links,
                               args.sameline, args.sort)
    treekws = [loc.kws() for loc in args.locations]
    with FileHashTree.listof(treekws) as all_trees:
        if hash_depends_on_file_size():
            FileHashTree.scan_trees_async(all_trees)
            for file_sz in sorted(fdupes.sizes_onall(all_trees)):
                with pr.ProgressPrefix("size %s:" % (bytes2human(file_sz),)):
                    for _hash, located_files in \
                            fdupes.located_files_onall_of_size(all_trees, file_sz):
                        return_code = 0
                        grouper.add_group(located_files)
        else:
            for _hash, located_files in \
                    fdupes.located_files_onall_of_size(all_trees, None):
                return_code = 0
                grouper.add_group(located_files)
        grouper.flush()
    return return_code

def do_onfirstonly(args):
    return_code = 1 # Default if none are found.
    grouper = \
        GroupedFileListPrinter(args.hard_links, args.all_links,
                               args.sameline, args.sort)
    with FileHashTree.listof(loc.kws() for loc in args.locations) as all_trees:
        if hash_depends_on_file_size():
            FileHashTree.scan_trees_async(all_trees)
            first_tree = all_trees[0]
            other_trees = all_trees[1:]
            for file_sz in sorted(first_tree.get_all_sizes()):
                with pr.ProgressPrefix("size %s:" % (bytes2human(file_sz),)):
                    if not any(tr.size_to_files(file_sz) for tr in other_trees):
                        for fobj in first_tree.size_to_files_gen(file_sz):
                            grouper.add_group({first_tree: [fobj]})
                        continue
                    for _hash, located_files in \
                            fdupes.located_files_onfirstonly_of_size(
                                    all_trees, file_sz):
                        return_code = 0
                        grouper.add_group(located_files)
        else:
            for _hash, located_files in \
                    fdupes.located_files_onfirstonly_of_size(all_trees, None):
                return_code = 0
                grouper.add_group(located_files)
        grouper.flush()
    return return_code

def do_onlastonly(args):
    locs = args.locations
    locs[0], locs[-1] = locs[-1], locs[0]
    return do_onfirstonly(args)

def do_onfirstnotonly(args):
    return_code = 1 # Default if none are found.
    grouper = \
        GroupedFileListPrinter(args.hard_links, args.all_links,
                               args.sameline, args.sort)
    with FileHashTree.listof(loc.kws() for loc in args.locations) as all_trees:
        if hash_depends_on_file_size():
            FileHashTree.scan_trees_async(all_trees)
            first_tree = all_trees[0]
            other_trees = all_trees[1:]
            for file_sz in sorted(first_tree.get_all_sizes()):
                with pr.ProgressPrefix("size %s:" % (bytes2human(file_sz),)):
                    if not any(tr.size_to_files(file_sz) for tr in other_trees):
                        continue
                    for _hash, located_files in \
                            fdupes.located_files_onfirstnotonly_of_size(
                                    all_trees, file_sz):
                        return_code = 0
                        grouper.add_group(located_files)
        else:
            raise NotImplementedError
        grouper.flush()
    return return_code

def do_onlastnotonly(args):
    locs = args.locations
    locs[0], locs[-1] = locs[-1], locs[0]
    return do_onfirstnotonly(args)

def do_aliases(args):
    """
    Handler for printing all alias.
    """
    with FileHashTree(**args.location.kws()) as tree:
        tree.scan_subtree() # Must scan full tree to find all aliases.
        file_obj = tree.path_to_obj(args.relpath)
        file_path_printable = args.relpath
        if file_obj is None:
            pr.error("path does not exist: " + file_path_printable)
        elif not file_obj.is_file():
            pr.error("not a file: " + file_path_printable)
        else:
            for path in file_obj.relpaths:
                pr.print(path)

def make_treekwargs(location, dbprefix=None):
    """
    Create a treekwargs dictionary with topdir_path, dbmaker, dbkwargs.

    Used in the tests as as well as in do_subdir.
    """
    if dbprefix is None:
        dbprefix = get_default_dbprefix()
    tree_arg = TreeLocationOnline(location)
    tree_arg.set_dbprefix(dbprefix)
    return tree_arg.kws()

def do_subdir(args):
    kwargs = args.topdir.kws()
    dbprefix = args.topdir.get_dbprefix()
    src_dir = kwargs["topdir_path"]
    src_dbpath = kwargs["dbkwargs"]["dbpath"]
    tgt_dir = os.path.join(src_dir, args.relativesubdir)
    tgt_dbpath = os.path.join(tgt_dir, pick_db_basename(tgt_dir, dbprefix))
    top_idc = make_id_computer(src_dir)
    if not top_idc.subdir_invariant:
        msg = "no subdir command for file system = " + top_idc.file_sys
        raise NotImplementedError(msg)
    with SQLPropDBManager(src_dbpath, mode=Mode.ONLINE) as src_db:
        with SQLPropDBManager(tgt_dbpath, mode=Mode.ONLINE) as tgt_db:
            src_db.merge_prop_values_into(tgt_db)
    with FileHashTree(**make_treekwargs(tgt_dir, dbprefix)) \
            as tgt_tree:
        tgt_tree.db_purge_old_entries()
        tgt_tree.db.compact()

def do_mkoffline(args):
    """
    Create an offline db by updating an online tree, copying it to
    the provided output filename and inserting file tree directory
    structure and file metadata into the outputm, offline db.
    Overwrites any file at the output.
    """
    # outputpath is either a writable file or empty, bar sync race conditions.
    if os.path.isfile(args.outputpath):
        if args.forcewrite:
            os.remove(args.outputpath)
        else:
            msg = f"will not overwrite without the -f option: {args.outputpath}"
            raise TreeError(msg)
    dbdir = args.sourcedir.compute_dbdir()
    tmpdir = None

    def filter_if_has_property(fid):
        fobj = src_tree.id_to_file(fid)
        if fobj is None:
            return False
        else:
            return src_tree.id_to_file(fid).prop_value is not None

    try:
        if not os.access(dbdir, os.W_OK):
            pr.warning(f"no write access to {dbdir}; using a temp database")
            tmpdir = tempfile.mkdtemp(prefix='lnsync-tmp-database')
            args.sourcedir.set_dblocation(os.path.join(tmpdir, 'tmp.db'))
        with FileHashTree(**args.sourcedir.kws()) as src_tree:
            src_tree.db_update_all()
            with SQLPropDBManager(args.outputpath, mode=Mode.OFFLINE) as tgt_db:
                src_tree.db_store_offline(
                    tgt_db,
                    filter_fn=filter_if_has_property)
                pr.progress("compacting database...")
                tgt_db.compact()
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir)


def do_cleandb(args):
    """
    Purge old entries from db and compact it.
    """
    with FileHashTree(**args.location.kws()) as tree:
        def describe_db(prefix):
            file_stat = os.stat(tree.db.dbpath)
            props = tree.db.count_prop_entries()
            pr.print(f"{prefix}file size: {bytes2human(file_stat.st_size)}, " \
                     f"hashes: {props}")
        pr.print("database file:", tree.db.dbpath)
        describe_db("before: ")
        pr.progress("removing offline data")
        tree.db.rm_offline_tree()
        pr.progress("purging old entries")
        tree.db_purge_old_entries()
        pr.progress("compacting database")
        tree.db.compact()
        describe_db("after: ")
