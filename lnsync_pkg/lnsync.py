#!/usr/bin/env python

"""Sync target file tree with source tree using hardlinks.
Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

from __future__ import print_function

import os
import argparse
import sys
import pipes
from sqlite3 import Error as SQLError

from six import raise_from, reraise, iteritems
from six.moves import reduce

from lnsync_pkg.p23compat import fstr, fstr2str
import lnsync_pkg.printutils as pr
import lnsync_pkg.fdupes as fdupes
import lnsync_pkg.metadata as metadata
from lnsync_pkg.human2bytes import human2bytes
from lnsync_pkg.sqlpropdb  import SQLPropDBManager
from lnsync_pkg.prefixdbname import \
    mode_from_location, pick_db_basename, set_prefix, mk_online_db
from lnsync_pkg.hashtree import FileHashTree, TreeError, PropDBValueError
from lnsync_pkg.matcher import TreePairMatcher
from lnsync_pkg.fileid import make_id_computer

# Tell pylint not to mistake module variables for constants
# pylint: disable=C0103

DEFAULT_DBPREFIX = fstr("lnsync-")
set_prefix(DEFAULT_DBPREFIX)

_quoter = pipes.quote

def wrap(text, width):
    """A word-wrap function that preserves existing line breaks
    and most spaces in the text. Expects that existing line
    breaks are posix newlines (\n).
    By Mike Brown, licensed under the PSF.
    """
    return reduce(lambda line, word, width=width: '%s%s%s' %
                  (line,
                   ' \n'[(len(line)-line.rfind('\n')-1
                          + len(word.split('\n', 1)[0]
                               ) >= width)],
                   word,),
                  text.split(' '),)

DESCRIPTION = wrap(
    "lnsync version %s on Python %d.%d.\n%s\n"
    "lnsync is Copyright (C) 2018 Miguel Simoes. "
    "This program comes with ABSOLUTELY NO WARRANTY. This is free software, "
    "and you are welcome to redistribute it under certain conditions. "
    "See the GNU General\n Public Licence v3 for details.\n"
    "More info at http://github.com/mrsimoes/lnsync" \
    % (metadata.version,
       sys.version_info[0], sys.version_info[1],
       metadata.description,),
    80
    )

# Verbosity subparser and control Action for both -q and -v
# belong to the main parser, not any command parser.

class SetVerbosityAction(argparse.Action):
    """Adjust verbosity level of print module."""
    def __init__(self, nargs=0, **kw):
        """Ensure this action consumes zero parameters consumed,
        just like store_true."""
        # Override default behavior of switch consuming one argument following.
        super(SetVerbosityAction, self).__init__(nargs=0, **kw)
    def __call__(self, parser, namespace, values, option_string=None):
        verb_delta = getattr(namespace, "verbosity_delta", 0) # Get running sum.
        if "q" in option_string:
            verb_delta -= 1
        elif  "v" in option_string:
            verb_delta += 1
        else:
            raise ValueError("parsing verbosity option")
        setattr(namespace, "verbosity_delta", verb_delta)

verbosity_options_parser = argparse.ArgumentParser(add_help=False)
verbosity_options_parser.add_argument(
    "-q", "--quiet", action=SetVerbosityAction, help="decrease verbosity")
verbosity_options_parser.add_argument(
    "-v", "--verbose", action=SetVerbosityAction, help="increase verbosity")

# All options following are shared by multiple command parsers.

# These options are unrelated to tree locations:

hardlinks_option_parser = argparse.ArgumentParser(add_help=False)
hardlinks_option_parser.add_argument(
    "-H", "--hardlinks", action="store_true",
    help="hardlinks are duplicates")

sameline_option_parser = argparse.ArgumentParser(add_help=False)
sameline_option_parser.add_argument(
    "-1", "--sameline", action="store_true",
    help="print each group of identical files in the same line")

# All options following apply to tree location arguments.

# --dbprefix affects tree locations placed after it
# Depends on argparse setting the default value at the outset and then
# updating it for action="store".)

dbprefix_option_parser = argparse.ArgumentParser(add_help=False)
dbprefix_option_parser.add_argument(
    "-p", "--dbprefix", metavar="PREFIX", type=fstr, action="store",
    default=DEFAULT_DBPREFIX,
    help="database filename prefix for following online trees")

# Any --exclude option applies to all tree locations anywhere in the command
# line.
# Any --exclude-once option applies only to the next tree location.
# Due to an argparse limitation, --exclude-once is not supported where
# for the elements of a tree location list (nargs="+"). argparse cannot parse
# optional arguments interspersed with the elements in a list argument.

# argparse initializes to None instead of an empty list.
exclude_option_parser = argparse.ArgumentParser(add_help=False)
exclude_option_parser.add_argument(
    "--exclude", metavar="GLOBPATTERN", type=fstr, action="append",
    help="exclude certain files and dirs")

# argparse autochanges option name to "exclude_once":
excludeonce_option_parser = argparse.ArgumentParser(add_help=False)
excludeonce_option_parser.add_argument(
    "--exclude-once", metavar="GLOBPATTERN", type=fstr, action="append",
    help="exclude for the following tree only")

def tree_location(location, mandatory_mode=None):
    """Transforms a location path string to a tuple (location, mode).
    Also, get rid of Python 3 UTF8 filename surrogate-twisting madness.
    """
    location = fstr(location)
    mode = mode_from_location(location, mandatory_mode)
    return (location, mode)

def tree_online(location):
    return tree_location(location, "online")

def tree_offline(location):
    return tree_location(location, "offline")

# Actions that store tree location data in a dict.

def new_tree_spec(location, mode, dbprefix):
    """Create a tree spec dict, return it."""
    tree_spec = {"cmd_location": location,
                 "real_location": os.path.realpath(location),
                 "mode": mode,
                 "dbprefix": dbprefix,
                 "exclude_patterns": []}
    return tree_spec

class StoreTreeSpec(argparse.Action):
    """Store a tree spec from location/mode/dbprefix data.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        """Save a tree spec from values (location, mode) and current dbprefix.
        Include and consume --exclude-once options.
        Global--exclude patterns are added at the end of parsing.
        """
        assert not isinstance(values, list)
        dbprefix = namespace.dbprefix
        tree_spec = new_tree_spec(values[0], values[1], dbprefix)
        self.store_tree_spec(namespace, tree_spec)
        once_excludes = getattr(namespace, "exclude_once", None)
        if once_excludes is not None:
            treekw = "exclude_patterns"
            tree_spec[treekw] = list(once_excludes)
            delattr(namespace, "exclude_once")
        setattr(namespace, self.dest, tree_spec)

    def store_tree_spec(self, namespace, tree_spec):
        """Add tree_spec to a global list.
        """
        all_tree_specs = getattr(namespace, "all_tree_specs", [])
        all_tree_specs.append(tree_spec)
        setattr(namespace, "all_tree_specs", all_tree_specs)

class StoreTreeSpecList(StoreTreeSpec):
    """Create a list of one or more tree specs."""
    def __call__(self, parser, namespace, values, option_string=None):
        dbprefix = namespace.dbprefix
        assert isinstance(values, list)
        tree_specs = \
            [new_tree_spec(val[0], val[1], dbprefix) for val in values]
        for tr in tree_specs:
            self.store_tree_spec(namespace, tr)
        setattr(namespace, self.dest, tree_specs)

class StoreRoot(argparse.Action):
    """Store another root with active dbprefix."""
    def __call__(self, parser, namespace, values, option_string=None):
        assert not isinstance(values, list) # A single root argument at a time.
        prev_roots = getattr(namespace, self.dest)
        if not prev_roots: # argparse defaults to explicit None.
            prev_roots = []
        dbprefix = namespace.dbprefix
        tree_spec = new_tree_spec(values[0], values[1], dbprefix)
        prev_roots.append(tree_spec)
        setattr(namespace, self.dest, prev_roots) # Store db spec in Namespace.

# Create treekwargs with appropritate kwargs to init an
# instance of an online/offline file tree from stored location data.

def make_treekwargs(location, mode="online", dbprefix=DEFAULT_DBPREFIX):
    """Return new treekwargs with root_path, dbmaker, dbkwargs.
    """
    tree_spec = new_tree_spec(location, mode, dbprefix)
    tree_spec_to_kwargs(tree_spec) # Changed in-place.
    return tree_spec

def tree_spec_to_kwargs(tree_spec):
    """Edit tree data dict with dbmaker, dbkwargs, root_path.
    """
    real_location = tree_spec["real_location"]
    del tree_spec["cmd_location"]
    del tree_spec["real_location"]
    dbprefix = tree_spec["dbprefix"]
    del tree_spec["dbprefix"]
    if tree_spec["mode"] == "online":
        dbpath = os.path.join(real_location,
                              pick_db_basename(real_location, dbprefix))
        tree_spec["root_path"] = real_location
        tree_spec["dbkwargs"] = {"dbpath":dbpath, "root_path":real_location}
    else:
        dbpath = real_location
        tree_spec["root_path"] = None
        tree_spec["dbkwargs"] = {"dbpath":dbpath, "root_path":None}
    tree_spec["dbmaker"] = SQLPropDBManager


# Options that apply to all tree locations.

root_option_parser = argparse.ArgumentParser(add_help=False)
root_option_parser.add_argument(
    "--root", metavar="DIR", type=tree_online, action=StoreRoot,
    help="read and update root database for all subtree locations")

bysize_option_parser = argparse.ArgumentParser(add_help=False)
bysize_option_parser.add_argument(
    "-z", "--bysize", default=False, action="store_true",
    help="compare files by size only")

maxsize_option_parser = argparse.ArgumentParser(add_help=False)
maxsize_option_parser.add_argument(
    "-M", "--maxsize", type=human2bytes, default=-1,
    help="ignore files larger than MAXSIZE (default is no limit) "
         "suffixes allowed: K, M, G, etc.")

skipempty_option_parser = argparse.ArgumentParser(add_help=False)
skipempty_option_parser.add_argument(
    "-0", "--skipempty", default=False, action="store_true",
    help="ignore empty files")

# Options that apply to all tree locations.

def finish_parsing_trees(args):
    """Transform each tree spec into tree init kwargs, incorporating global
    --exclude and -root options. Also, add up and apply verbosity options.
    """
    def is_subdir(path, directory):
        "Test if path is under directory."
        relative = os.path.relpath(path, directory)
        return not relative.startswith(fstr(os.pardir + os.sep))
    # argparse may not set default value for --exclude option.
    verbosity_delta = getattr(args, "verbosity_delta", 0)
    pr.option_verbosity += verbosity_delta
    global_excludes = getattr(args, "exclude", None)
    root_specs = getattr(args, "root", [])
    if not root_specs: # argparse sets None as explicit default.
        root_specs = []
    for root_spec in root_specs:
        tree_spec_to_kwargs(root_spec)
    previous_locations = []
    tree_specs = getattr(args, "all_tree_specs", [])
    for tree_spec in tree_specs:
        this_location = tree_spec["real_location"]
        if this_location in previous_locations:
            raise ValueError(
                "duplicate location: %s" % (tree_spec["cmd_location"],))
        previous_locations.append(this_location)
        replacement_db = None
        for root_spec in root_specs:
            root_dir = root_spec["root_path"]
            if tree_spec["mode"] == "online" \
                    and is_subdir(this_location, root_dir):
                replacement_db = root_spec["dbkwargs"]["dbpath"]
                pr.info("using %s for %s" %
                        (fstr2str(replacement_db), fstr2str(this_location)))
                break
        tree_spec_to_kwargs(tree_spec)
        treekwargs = tree_spec # Just for clarity, since the contents changed.
        if replacement_db:
            treekwargs["dbkwargs"]["dbpath"] = replacement_db
        if global_excludes:
            treekw = "exclude_patterns"
            once_excludes = treekwargs.get(treekw, [])
            treekwargs[treekw] = global_excludes + once_excludes
        for arg in "bysize", "maxsize", "skipempty":
            # argparse argument names may not match FileHashTree _init_ kwargs:
            treekw = {"bysize":"size_as_hash",
                      "maxsize":"maxsize",
                      "skipempty":"skipempty"}[arg]
            if hasattr(args, arg):
                assert not treekw in treekwargs, "already set: "+str(treekw)
                treekwargs[treekw] = getattr(args, arg)

# Argument check and type coerce functions:

def relative_path(value):
    """Argument type to exclude absolute paths.
    """
    if os.path.isabs(value):
        raise argparse.ArgumentTypeError("not a relative path: %s." % value)
    return fstr(value)

def writable_empty_path(path):
    if os.path.isfile(path):
        msg = "file already exists at %s" % (path,)
        raise argparse.ArgumentTypeError(msg)
    try:
        f = open(path, 'w')
    except OSError as exc:
        msg = "cannot write to %s" % (path,)
        raise_from(argparse.ArgumentTypeError(msg), exc)
    else:
        f.close()
        os.remove(path)
        return fstr(path)

# Main parser and subcommand parsers.

# Register command handler functions here for the main body:
cmd_handlers = {}  # Commands fully argparsed
cmd_handlers_extra_args = {} # Commands taking extra non-argparsed arguments.

HELP_SPACING = 26

top_parser = argparse.ArgumentParser(\
    description=DESCRIPTION,
    parents=[verbosity_options_parser],
    formatter_class=lambda prog: argparse.HelpFormatter(
        prog, max_help_position=HELP_SPACING))
cmd_parsers = top_parser.add_subparsers(dest="cmdname", help="sub-command help")

## sync
parser_sync = cmd_parsers.add_parser(
    'sync',
    parents=[root_option_parser,
             exclude_option_parser,
             excludeonce_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
            ],
    help="sync target dir from source by content, by renaming and (de)linking "
         "on target, no file data copied or deleted, like a best-effort "
         "'rsync source/ target'")
parser_sync.add_argument("-n", "--dry-run", help="dry run", action="store_true")
parser_sync.add_argument(
    "source", type=tree_location, action=StoreTreeSpec)
parser_sync.add_argument(
    "targetdir", type=tree_online, action=StoreTreeSpec)
def do_sync(args):
    with FileHashTree(**args.source) as src_tree:
        with FileHashTree(**args.targetdir) as tgt_tree:
            matcher = TreePairMatcher(src_tree, tgt_tree)
            pr.progress("calculating match")
            if not matcher.do_match():
                raise NotImplementedError("match failed")
            pr.progress("syncing files")
            tgt_tree.writeback = not args.dry_run
            for cmd in matcher.generate_sync_cmds():
                cmd_str = \
                    "%s %s" % \
                        (cmd[0],
                         " ".join(_quoter(fstr2str(arg)) for arg in cmd[1:]))
                pr.print(cmd_str)
                try:
                    tgt_tree.exec_cmd(cmd)
                except OSError as exc: # E.g. if no linking support on target.
                    msg = "could not execute: " + cmd_str
                    raise_from(RuntimeError(msg), exc)
            pr.progress("syncing empty dirs")
            dirs_to_rm_set = set()
            dirs_to_rm_list = []
            for dir_obj, _parent_obj, relpath \
                    in tgt_tree.walk_paths(
                            recurse=True, topdown=False,
                            dirs=True, files=False):
                if all((obj.is_dir() and obj in dirs_to_rm_set)
                       for obj in dir_obj.entries.values()):
                    if src_tree.follow_path(relpath) is None:
                        dirs_to_rm_set.add(dir_obj)
                        dirs_to_rm_list.append(dir_obj)
            for d in dirs_to_rm_list:
                pr.print("rmdir %s" % (_quoter(fstr2str(d.get_relpath()),)))
                tgt_tree.rm_dir_writeback(d)
            pr.debug("sync done")
cmd_handlers["sync"] = do_sync

# The next few commands print out list of file paths.
class GroupedFileListPrinter(object):
    """Output filepaths in groups.
    If sameline is False, filenames are printed each on its line,
    with an empty line separating consecutive groups.
    If sameline is True, filenames in each group are printed on the same line,
    separated by spaces, with filename spaces and backslashes escaped.
    If hardlinks is True, print all aliases for each file as if they were
    different paths.
    If hardlinks is False, print a for each file a single alias, arbitrarily
    chosen.
    """
    def __init__(self, hardlinks, sameline):
        self.hardlinks = hardlinks
        self.sameline = sameline
        self._output_group_linebreak = False # Not before first group.
        if self.sameline:
            self._built_line = None
    def start_group(self):
        if self.sameline:
            self._built_line = ""
        else:
            if self._output_group_linebreak:
                pr.print("")
            else:
                self._output_group_linebreak = True
    def end_group(self):
        if self.sameline:
            pr.print(self._built_line)
    def print_located_files(self, located_files):
        for tree, fobjs in iteritems(located_files):
            for fobj in fobjs:
                self.print_file(tree, fobj)
    def print_file(self, tree, fobj):
        if self.sameline:
            if self._built_line != "":
                self._built_line += " "
            for k, relpath in enumerate(fobj.relpaths):
                if k == 0:
                    include, prefix = (True, "")
                elif self.hardlinks:
                    include, prefix = (True, " ")
                else:
                    include = False
                if include:
                    pr_path = tree.printable_path(relpath)
                    # Escape single backslashes.
                    pr_path = pr_path.replace("\\", "\\\\")
                    pr_path = pr_path.replace(r" ", r"\ ")
                    self._built_line += prefix + pr_path
        else:
            for k, relpath in enumerate(fobj.relpaths):
                if k == 0:
                    include, prefix = (True, "")
                elif self.hardlinks:
                    include, prefix = (True, "")
                else:
                    include = False
                if include:
                    pr_path = tree.printable_path(relpath)
                    pr.print(prefix, pr_path, end="\n")

## fdupes
parser_fdupes = cmd_parsers.add_parser(
    'fdupes',
    parents=[root_option_parser,
             exclude_option_parser,
             hardlinks_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
             sameline_option_parser],
    help='find duplicate files')
parser_fdupes.add_argument(
    "locations", type=tree_location, action=StoreTreeSpecList, nargs="+")
def do_fdupes(args):
    """Find duplicate files, using file size as well as file hash.
    """
    with FileHashTree.listof(args.locations) as all_trees:
        grouper = GroupedFileListPrinter(args.hardlinks, args.sameline)
        for file_sz in fdupes.sizes_repeated(all_trees, args.hardlinks):
            with pr.ProgressPrefix("size %d:" % (file_sz,)):
                for _hash, located_files in \
                    fdupes.located_files_repeated_of_size(
                            all_trees, file_sz, args.hardlinks):
                    grouper.start_group()
                    grouper.print_located_files(located_files)
                    grouper.end_group()
cmd_handlers["fdupes"] = do_fdupes

## onall
parser_onall = cmd_parsers.add_parser(
    'onall',
    parents=[root_option_parser,
             exclude_option_parser,
             hardlinks_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
             sameline_option_parser],
    help='find files common to all trees')
parser_onall.add_argument(
    "locations", type=tree_location, action=StoreTreeSpecList, nargs="+")
def do_onall(args):
    with FileHashTree.listof(args.locations) as all_trees:
        grouper = GroupedFileListPrinter(args.hardlinks, args.sameline)
        for file_sz in fdupes.sizes_onall(all_trees):
            with pr.ProgressPrefix("size %d:" % (file_sz,)):
                for _hash, located_files in \
                        fdupes.located_files_onall_of_size(all_trees, file_sz):
                    grouper.start_group()
                    grouper.print_located_files(located_files)
                    grouper.end_group()
cmd_handlers["onall"] = do_onall

## onfirstonly
parser_onfirstonly = cmd_parsers.add_parser(
    'onfirstonly',
    parents=[root_option_parser,
             exclude_option_parser,
             hardlinks_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
             sameline_option_parser],
    help='find files on first tree, not on any other')
parser_onfirstonly.add_argument(
    "locations", type=tree_location, action=StoreTreeSpecList, nargs="+")
def do_onfirstonly(args):
    with FileHashTree.listof(args.locations) as all_trees:
        grouper = GroupedFileListPrinter(args.hardlinks, args.sameline)
        for file_sz in all_trees[0].get_all_sizes():
            with pr.ProgressPrefix("size %d:" % (file_sz,)):
                for _hash, located_files in \
                        fdupes.located_files_onfirstonly_of_size(
                                all_trees, file_sz):
                    grouper.start_group()
                    grouper.print_located_files(located_files)
                    grouper.end_group()
cmd_handlers["onfirstonly"] = do_onfirstonly

## onlastonly
parser_onlastonly = cmd_parsers.add_parser(
    'onlastonly',
    parents=[root_option_parser,
             exclude_option_parser,
             hardlinks_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
             sameline_option_parser],
    help='find files on last tree, not on any other')
parser_onlastonly.add_argument(
    "locations", type=tree_location, action=StoreTreeSpecList, nargs="+")
def do_onlastonly(args):
    locs = args.locations
    locs[0], locs[-1] = locs[-1], locs[0]
    do_onfirstonly(args)
cmd_handlers["onlastonly"] = do_onlastonly

## cmp
parser_cmp = cmd_parsers.add_parser(
    'cmp',
    parents=[root_option_parser,
             exclude_option_parser,
             excludeonce_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
            ],
    help='recursively compare two trees')
parser_cmp.add_argument(
    "leftlocation", type=tree_location, action=StoreTreeSpec)
parser_cmp.add_argument(
    "rightlocation", type=tree_location, action=StoreTreeSpec)
def do_cmp(args):
    """Recursively compare files and dirs in two directories.
    """
    def cmp_files(path, left_obj, right_obj):
        left_prop, right_prop = None, None
        try:
            left_prop = left_tree.get_prop(left_obj)
            right_prop = right_tree.get_prop(right_obj)
        except TreeError:
            if left_prop is None:
                err_path = left_tree.printable_path(path, pprint=_quoter)
            else:
                err_path = right_tree.printable_path(path, pprint=_quoter)
            pr.error("reading %s, ignoring" % (err_path,))
        else:
            if left_prop != right_prop:
                pr.print("files differ: %s" % fstr2str(path,))
            else:
                pr.info("files equal: %s" % fstr2str(path,))
    def cmp_subdir(cur_dirpath):
        for left_obj, basename in \
                left_tree.walk_dir_contents(cur_dirpath, dirs=True):
            left_path = os.path.join(cur_dirpath, basename)
            right_obj = right_tree.follow_path(left_path)
            if right_obj is None:
                if left_obj.is_file():
                    left_path_printable = \
                        left_tree.printable_path(left_path, pprint=_quoter)
                    pr.print("left file only: %s" % (left_path_printable,))
                elif left_obj.is_dir():
                    left_path_printable_dir = \
                        left_tree.printable_path(
                            left_path+fstr(os.path.sep), pprint=_quoter)
                    pr.print("left dir only: %s" % (left_path_printable_dir,))
                else:
                    raise RuntimeError(
                        "unexpected left object: " + fstr2str(left_path))
            elif left_obj.is_file():
                if  right_obj.is_file():
                    cmp_files(left_path, left_obj, right_obj)
                elif right_obj.is_dir():
                    pr.print("left file vs right dir: %s" % fstr2str(left_path))
                else:
                    pr.print("left file vs other: %s" % fstr2str(left_path))
            elif left_obj.is_dir():
                if right_obj.is_dir():
                    dirpaths_to_visit.append(left_path)
                elif right_obj.is_file():
                    pr.print("left dir vs right file: %s" % fstr2str(left_path))
                else:
                    pr.print("left dir vs other: %s" % fstr2str(left_path,))
            else:
                raise RuntimeError(
                    "unexpected left object: " + fstr2str(left_path))
        for right_obj, basename in \
                right_tree.walk_dir_contents(cur_dirpath, dirs=True):
            right_path = os.path.join(cur_dirpath, basename)
            left_obj = left_tree.follow_path(right_path)
            if left_obj is None:
                if right_obj.is_file():
                    right_path_printable = \
                        right_tree.printable_path(right_path, pprint=_quoter)
                    pr.print("right file only: %s" % (right_path_printable,))
                elif right_obj.is_dir():
                    right_path_printable_dir = \
                        right_tree.printable_path(
                            right_path+fstr(os.path.sep), pprint=_quoter)
                    pr.print("right dir only: %s" % (right_path_printable_dir,))
                else:
                    raise RuntimeError(
                        "unexpected right object: " + fstr2str(right_path))
            elif right_obj.is_file():
                if not left_obj.is_file() and not left_obj.is_dir():
                    pr.print(
                        "left other vs right file: %s" % fstr2str(right_path))
            elif right_obj.is_dir():
                if not left_obj.is_file() and not left_obj.is_dir():
                    pr.print(
                        "left other vs right dir: %s" % fstr2str(right_path))
            else:
                raise RuntimeError(
                    "unexpected right object: " + fstr2str(right_path))
    with FileHashTree(**args.leftlocation) as left_tree:
        with FileHashTree(**args.rightlocation) as right_tree:
            dirpaths_to_visit = [fstr("")]
            while dirpaths_to_visit:
                cur_dirpath = dirpaths_to_visit.pop()
                cmp_subdir(cur_dirpath)
cmd_handlers["cmp"] = do_cmp

## lookup
parser_lookup = \
    cmd_parsers.add_parser(
        'lookup', parents=[dbprefix_option_parser],
        help='retrieve file hash')
parser_lookup.add_argument("location", type=tree_location, action=StoreTreeSpec)
parser_lookup.add_argument("relpath", type=relative_path)
def do_lookup(args):
    "Handler for looking up a fpath hash in the tree."
    with FileHashTree(**args.location) as tree:
        fpath = args.relpath
        fobj = tree.follow_path(fpath)
        if fobj is None or not fobj.is_file():
            pr.warning("not a file: %s" % (tree.printable_path(
                fpath, pprint=_quoter),))
        else:
            hash_val = tree.get_prop(fobj)
            pr.print(hash_val)
cmd_handlers["lookup"] = do_lookup

## check
parser_check_files = cmd_parsers.add_parser(
    'check',
    parents=[root_option_parser,
             exclude_option_parser,
             hardlinks_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser],
    help='rehash files and compare against stored value')
parser_check_files.add_argument(
    "location", type=tree_online, action=StoreTreeSpec)
parser_check_files.add_argument(
    "relpaths", type=relative_path, nargs="*")

def do_check(args):
    with FileHashTree(**args.location) as tree:
        assert tree.db.mode == "online", "do_check tree not online"
        which_files_gen = args.relpaths
        if not which_files_gen:
            def gen_all_paths():
                for _obj, _parent, path in tree.walk_paths(
                        files=True, dirs=False, recurse=True):
                    yield path
            which_files_gen = gen_all_paths()
        file_objs_checked_ok = set()
        file_objs_checked_bad = set()
        try:
            for path in which_files_gen:
                pr.progress("checking: %s" % path)
                fobj = tree.follow_path(path)
                if fobj in file_objs_checked_ok:
                    continue
                if fobj in file_objs_checked_bad and not args.hardlinks:
                    continue
                try:
                    res = tree.db_check_prop(fobj)
                except PropDBValueError:
                    pr.warning("not checked: '%s'" % fstr2str(path))
                    continue
                except TreeError as exc:
                    pr.error("while checking %s: %s" % (fstr2str(path), str(exc)))
                    continue
                if res:
                    file_objs_checked_ok.add(fobj)
                else:
                    pr.print("failed check: %s" % path)
                    file_objs_checked_bad.add(fobj)
        except KeyboardInterrupt as e:
            t, v, tb = sys.exc_info()
            pr.print("Interrupted... ", end="")
            reraise(KeyboardInterrupt, v, tb)
        finally:
            tot_files_checked = len(file_objs_checked_ok) \
                              + len(file_objs_checked_bad)
            tot_files_failed = len(file_objs_checked_bad)
            msg_out_tail = "/ %d files checked" % (tot_files_checked,)
            if tot_files_failed > 0:
                pr.print("%d file(s) failed %s" % \
                    (tot_files_failed, msg_out_tail))
                for p in file_objs_checked_bad:
                    pr.print(p.relpaths[0])
            else:
                pr.info("no files failed %s" % (msg_out_tail,))

cmd_handlers["check"] = do_check

parser_rsync = cmd_parsers.add_parser(
    'rsync',
    parents=[root_option_parser,
             exclude_option_parser,
             dbprefix_option_parser,
             maxsize_option_parser
            ],
    help="print rsync command (skipping hash database file)")
parser_rsync.add_argument(
    "-x", "--execute", action="store_true", help="also execute rsync command")
parser_rsync.add_argument(
    "-n", "--dry-run", help="dry run", action="store_true")
parser_rsync.add_argument("sourcedir", type=str)
parser_rsync.add_argument("targetdir", type=str)

def do_rsync(sysargv, args, more_args):
    """Print suitable rsync command.
    """
    if more_args and more_args != sysargv[-len(more_args):]:
        top_parser.parse_args()
        assert False, "internal error"
    else:
        rsyncargs = more_args
    src_dir, tgt_dir = args.sourcedir, args.targetdir
    if src_dir[-1] != os.sep:
        src_dir += os.sep # rsync needs trailing / on sourcedir.
    while tgt_dir[-1] == os.sep:
        tgt_dir = tgt_dir[:-1]
    src_dir = _quoter(src_dir)
    tgt_dir = _quoter(tgt_dir)
    # Options for rsync: recursive, preserve hardlinks.
    rsync_opts = "-r -H --size-only --progress"
    if args.maxsize > 0:
        rsync_opts += " --max-size=%d" % args.maxsize
    if args.dry_run:
        rsync_opts += " -n"
    exclude_patterns = args.exclude
    if exclude_patterns:
        for p in exclude_patterns:
            rsync_opts += ' --exclude="%s"' % fstr2str(p)
    # Exclude databases at both ends.
    rsync_opts += r' --exclude="/%s[0-9]*.db"' % fstr2str(args.dbprefix)
    if rsyncargs:
        rsync_opts += " " + " ".join(rsyncargs)
    rsync_cmd = "rsync %s %s %s" % (rsync_opts, src_dir, tgt_dir)
    pr.print(rsync_cmd)
    if args.execute:
        try:
            os.system(rsync_cmd)
        except OSError as exc:
            msg = "executing %s: %s" % (rsync_cmd, str(exc))
            pr.error(msg)
            raise_from(RuntimeError(msg), exc) # Chain exception.
cmd_handlers_extra_args["rsync"] = do_rsync

## mkoffline
parser_mkoffline = cmd_parsers.add_parser(
    'mkoffline',
    parents=[exclude_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser],
    help="create offline file tree from source dir")
parser_mkoffline.add_argument(
    "sourcedir", type=tree_online, action=StoreTreeSpec)
parser_mkoffline.add_argument(
    "outputpath", type=writable_empty_path)

def do_mkoffline(args):
    """Create an offline db by updating an online tree, copying it to
    the provided output filename and inserting file tree directory
    structure and file metadata into the outputm, offline db.
    Overwrites any file at the output.
    """
    with FileHashTree(**args.sourcedir) as src_tree:
        src_tree.db_update_all()
        with SQLPropDBManager(args.outputpath, mode="offline") as tgt_db:
            with pr.ProgressPrefix("saving: "):
                src_tree.db_store_offline(tgt_db)
            tgt_db.compact()
cmd_handlers["mkoffline"] = do_mkoffline

## update
parser_update = cmd_parsers.add_parser(
    'update',
    parents=[root_option_parser,
             exclude_option_parser,
             dbprefix_option_parser,
             skipempty_option_parser,
             maxsize_option_parser
            ],
    help='update hashes for new and modified files')
parser_update.add_argument(
    "dirs", type=tree_online, action=StoreTreeSpecList, nargs="+")
def do_update(args):
    with FileHashTree.listof(args.dirs) as trees:
        for tree in trees:
            tree.db_update_all()
cmd_handlers["update"] = do_update

## rehash
parser_rehash = cmd_parsers.add_parser(
    'rehash', parents=[dbprefix_option_parser],
    help='force hash updates for given files')
parser_rehash.add_argument(
    "topdir", type=tree_online, action=StoreTreeSpec)
parser_rehash.add_argument("relfilepaths", type=relative_path, nargs='+')
def do_rehash(args):
    with FileHashTree(**args.topdir) as tree:
        for relpath in args.relfilepaths:
            file_obj = tree.follow_path(relpath)
            if file_obj is None or not file_obj.is_file():
                pr.error("not a relative path to a file: %s" % str(relpath))
                continue
            try:
                tree.db_recompute_prop(file_obj)
            except TreeError as exc:
                pr.debug(str(exc))
                pr.error("while rehashing %s: %s" % \
                    (tree.printable_path(relpath, pprint=_quoter),
                     str(exc)))
                continue
cmd_handlers["rehash"] = do_rehash

## subdir
parser_subdir = \
    cmd_parsers.add_parser(
        'subdir',
        parents=[dbprefix_option_parser],
        help='copy hashes to new database at subdir')
parser_subdir.add_argument("topdir", type=fstr)
parser_subdir.add_argument("relativesubdir", type=relative_path)
def do_subdir(args):
    src_dir = args.topdir
    src_db = mk_online_db(src_dir, pick_db_basename(src_dir, args.dbprefix))
    tgt_dir = os.path.join(src_dir, args.relativesubdir)
    tgt_db = mk_online_db(tgt_dir, pick_db_basename(tgt_dir, args.dbprefix))
    top_idc = make_id_computer(src_dir)
    if not top_idc.subdir_invariant:
        msg = "no subdir command for file system = %s" % top_idc.file_sys
        raise NotImplementedError(msg)
    with src_db:
        with tgt_db:
            src_db.merge_prop_values(tgt_db)
    with FileHashTree(**make_treekwargs(tgt_dir, "online")) as tgt_tree:
        tgt_tree.db_purge_old_entries()
        tgt_tree.db.compact()
cmd_handlers["subdir"] = do_subdir

## cleandb
parser_cleandb = \
    cmd_parsers.add_parser(
        'cleandb',
        parents=[dbprefix_option_parser],
        help="clean and defragment the hash database at dir")
parser_cleandb.add_argument(
    "location", type=tree_online, action=StoreTreeSpec)
def do_cleandb(args):
    """Purge old entries from db and compact it.
    """
    with FileHashTree(**args.location) as tree:
        pr.progress("removing offline data")
        tree.db.rm_offline_tree()
        pr.progress("purging old entries")
        tree.db_purge_old_entries()
        pr.progress("compacting database")
        tree.db.compact()
cmd_handlers["cleandb"] = do_cleandb

def main():
    if len(sys.argv) == 1:
        top_parser.print_help(sys.stderr)
        sys.exit(1)
    pr.set_app_prefix("lnsync: ")
    try:
        args, extra_args = top_parser.parse_known_args()
        cmd = args.cmdname
        if cmd in cmd_handlers:
            args = top_parser.parse_args()
            finish_parsing_trees(args)
            handler_fn = lambda: cmd_handlers[cmd](args)
        elif cmd in cmd_handlers_extra_args:
            args, extra_args = top_parser.parse_known_args()
            finish_parsing_trees(args)
            handler_fn = \
                lambda: cmd_handlers_extra_args[cmd](
                    sys.argv[1:], args, extra_args)
        else:
            args = top_parser.parse_args()
            assert False, "error: error expected"
        try:
            handler_fn()
        except Exception as exc:
            if __debug__:
                print(type(exc), exc)
            raise
        sys.exit(0)
    except KeyboardInterrupt:
        raise SystemExit("lnsync: interrupted")
    except NotImplementedError as exc:
        pr.error("not implemented on your system: %s", str(exc))
    except RuntimeError as exc: # Includes NotImplementedError
        pr.error("runtime error: %s" % str(exc))
    except SQLError as exc:
        pr.error("database error: %s" % str(exc))
    except AssertionError as exc:
        pr.error("internal check failed: %s" % str(exc))
    except ValueError as exc:
        pr.error("bad argument: %s" % str(exc))
    except Exception as exc:
        pr.error("general exception: %s" % str(exc))
    finally:
        sys.exit(2)

if __name__ == "__main__":
    main()
