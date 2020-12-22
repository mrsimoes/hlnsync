#!/usr/bin/env python

"""
Sync target file tree with source tree using hardlinks.
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

import os
import argparse
import sys
import pipes
from sqlite3 import Error as SQLError

from functools import reduce

from lnsync_pkg.p23compat import fstr, fstr2str
import lnsync_pkg.printutils as pr
from lnsync_pkg.groupedfileprinter import GroupedFileListPrinter
import lnsync_pkg.fdupes as fdupes
from lnsync_pkg.argparseconfig import \
    ConfigError, NoSectionError, NoOptionError, \
    ArgumentParserConfig as ArgumentParser
import lnsync_pkg.metadata as metadata
from lnsync_pkg.modaltype import ONLINE, OFFLINE
from lnsync_pkg.human2bytes import human2bytes, bytes2human
from lnsync_pkg.sqlpropdb import mk_online_db, SQLPropDBManager
from lnsync_pkg.prefixdbname import \
    mode_from_location, pick_db_basename, set_prefix
from lnsync_pkg.hashtree import FileHashTree, \
    TreeError, PropDBException, PropDBError
from lnsync_pkg.matcher import TreePairMatcher
from lnsync_pkg.fileid import make_id_computer
from lnsync_pkg.glob_matcher import Pattern, IncludePattern, ExcludePattern

# Global variables.

# Tell pylint not to mistake module variables for constants
# pylint: disable=C0103

DEFAULT_DBPREFIX = fstr("lnsync-")
set_prefix(DEFAULT_DBPREFIX)
HELP_SPACING = 30
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

# NB: A new version of argparse reformats the description string to fit the
# terminal width.

DESCRIPTION = wrap(
    "lnsync %s (python %d.%d).\n%s\n"
    "Home: http://github.com/mrsimoes/lnsync "
    "Copyright (C) 2018 Miguel Simoes. "
    "This program comes with ABSOLUTELY NO WARRANTY. This is free software, "
    "and you are welcome to redistribute it under certain conditions. "
    "See the GNU General\n Public Licence v3 for details.\n" \
    % (metadata.version,
       sys.version_info[0], sys.version_info[1],
       metadata.description,),
    80
    )

# Argument parsing

# The top-level parser has a single optional argument: verbosity switches.

verbosity_options_parser = ArgumentParser(add_help=False)
class SetVerbosityAction(argparse.Action):
    """Adjust verbosity level of print module up or down."""
    def __init__(self, nargs=0, **kw):
        # Set nargs to zero.
        super(SetVerbosityAction, self).__init__(nargs=0, **kw)
    def __call__(self, parser, namespace, values, option_string=None):
        val = getattr(namespace, self.dest)
        if "q" in option_string:
            val -= 1
        elif "v" in option_string:
            val += 1
        else:
            raise ValueError("parsing verbosity option")
        setattr(namespace, self.dest, val)
verbosity_options_parser.add_argument(
    "-q", "--quiet", "-v", "--verbose",
    action=SetVerbosityAction, default=0, dest="verbosity_delta",
    help="decrease/increase verbosity")

configfile_option_parser = ArgumentParser(add_help=False)
class ChooseConfigfileAction(argparse.Action):
    """Adjust verbosity level of print module up or down."""
    def __init__(self, **kw):
        # Gets called twice, as it is the common action to two options.
        # No harm done.
        ArgumentParser.set_cfg_locations(
            "./lnsync.cfg",
            os.path.expanduser("~/lnsync.cfg"),
            os.path.expanduser("~/.lnsync.cfg"))
        super(ChooseConfigfileAction, self).__init__(**kw)
    def __call__(self, parser, namespace, values, option_string=None):
        if "--no-" in option_string:
            cfgfiles = ()
        else:
            cfgfiles = (namespace.configfile)
        try:
        # This must be done before all the add_argument calls below.
            ArgumentParser.set_cfg_locations(*cfgfiles)
        except ConfigError as e:
            pr.error("config file: %s" % (e))
            sys.exit(-1)
configfile_option_parser.add_argument(
    "--config", action=ChooseConfigfileAction,
    dest="configfile", default=None,
    help="choose configuration file")
configfile_option_parser.add_argument(
    "--no-config", action=ChooseConfigfileAction,
    nargs=0, dest="configfile")

# Overview of remainder argument parsing:
#
# Each lnsync command has one more positional arguments (directories
# and/or offline tree files), as well as optional arguments.
#
# Each optional argument is either unrelated to the positional arguments
# or applies to the following positionals or applies to all positionals.
#
# Shared optional arguments are handled by single-argument parsers.

maxsize_option_parser = ArgumentParser(add_help=False)
maxsize_option_parser.add_argument(
    "-M", "--maxsize",
    help="ignore files larger than MAXSIZE (default is no limit) "
         "suffixes allowed: K, M, G, etc.",
    type=human2bytes, default=-1)

hardlinks_option_parser = ArgumentParser(add_help=False)
hardlinks_option_parser.add_argument(
    "-H", "--hardlinks", dest="hardlinks", action="store_true", default=False,
    help="treat hardlinks/paths to the same file as distinct")
hardlinks_option_parser.add_argument(
    "--no-hardlinks", dest="hardlinks", action="store_false", default=False,
    help="treat hardlinks/paths to the same file as the same")
hardlinks_option_parser.add_argument(
    "-A", "--alllinks", dest="alllinks", action="store_true", default=False,
    help="on results, print all hardlinks, not just one")
hardlinks_option_parser.add_argument(
    "--no-alllinks", dest="alllinks", action="store_false", default=False,
    help="on results, choose an arbitrary hardlink to represent a file")

sameline_option_parser = ArgumentParser(add_help=False)
sameline_option_parser.add_argument(
    "-1", "--sameline", dest="sameline", action="store_true", default=False,
    help="print each group of identical files in the same line")
sameline_option_parser.add_argument(
    "--no-sameline", dest="sameline", action="store_false", default=False)

sort_option_parser = ArgumentParser(add_help=False)
sort_option_parser.add_argument(
    "-s", "--sort", dest="sort", action="store_true", default=False,
    help="sort output by size")
sort_option_parser.add_argument(
    "--no-sort", dest="sort", action="store_false", default=False)

bysize_option_parser = ArgumentParser(add_help=False)
bysize_option_parser.add_argument(
    "-z", "--bysize", dest="bysize", action="store_true", default=False,
    help="compare files by size only")
bysize_option_parser.add_argument(
    "--no-bysize", dest="bysize", action="store_false", default=False)

skipempty_option_parser = ArgumentParser(add_help=False)
skipempty_option_parser.add_argument(
    "-0", "--skipempty", dest="skipempty", action="store_true", default=False,
    help="ignore empty files")
skipempty_option_parser.add_argument(
    "--no-skipempty", dest="skipempty", action="store_false", default=False)

dryrun_option_parser = ArgumentParser(add_help=False)
dryrun_option_parser.add_argument(
    "-n", "--dry-run", dest="dry_run", default=False, action="store_true",
    help="dry run")
dryrun_option_parser.add_argument(
    "--no-dry-run", dest="dry_run", default=False, action="store_false")

# All optionals following apply to all tree positionals.

# --dbprefix affects tree locations placed following.
dbprefix_option_parser = ArgumentParser(add_help=False)
dbprefix_option_parser.add_argument(
    "-p", "--dbprefix", metavar="PREFIX", type=fstr, action="store",
    default=DEFAULT_DBPREFIX,
    help="database filename prefix for following online trees")

# All --exclude option applies to all tree locations in the command line.
# Each --exclude-once option applies only to the subsequent tree location.
# --exclude-once is not supported in between positionals in a list (nargs="+").

def make_pattern_obj_list(option_string, pattern_strings):
    """Transform a list of string patterns to a list of pattern objects.
    option_string either contains include or exclude."""
    def make_pattern_obj(option_string, pattern_str):
        if "include" in option_string:
            obj_type = IncludePattern
        elif "exclude" in option_string:
            obj_type = ExcludePattern
        else:
            raise RuntimeError
        return obj_type(pattern_str)
    return [make_pattern_obj(option_string, p) for p in pattern_strings]

class StoreIncExcPattern(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        """Create and store the appropriate Include or Exclude objects."""
        assert isinstance(values, list)
        prev_patterns = getattr(namespace, self.dest, None)
        prev_patterns += make_pattern_obj_list(option_string, values)
        setattr(namespace, self.dest, prev_patterns)

# Cannot create the Exclude and Include objects using a argument type casting
# function, because the option_string is required.

exclude_option_parser = ArgumentParser(add_help=False)
exclude_option_parser.add_argument(
    "--exclude", "--include", metavar="GLOBPATTERN",
    type=fstr, action=StoreIncExcPattern,
    default=[], nargs="+", dest="exclude",
    help="exclude/include files and dirs")

excludeonce_option_parser = ArgumentParser(add_help=False)
excludeonce_option_parser.add_argument(
    "--exclude-once", "--include-once", metavar="GLOBPATTERN",
    type=fstr, action=StoreIncExcPattern,
    default=[], nargs="+", dest="exclude_once",
    help="exclude/include for the following tree only, "
         "with precedence over global patterns")

class ArgTree:
    """Convert an argument an line location string to this type,
    checking if it is online or offline."""
    def __init__(self, location, mandatory_mode=None):
        location = fstr(location) # Use byte sequences to avoid Python3 UTF8.
        self.exclude_patterns = []
        self.cmd_location = location
        self.real_location = os.path.realpath(location)
        self.mode = mode_from_location(location, mandatory_mode)
        self._treekws = {"mode":self.mode,
                         "exclude_patterns":self.exclude_patterns}
        if self.mode == ONLINE:
            self._treekws["dbmaker"] = SQLPropDBManager
            self._treekws["root_path"] = self.real_location
        else:
            self._treekws["dbmaker"] = SQLPropDBManager
            self._treekws["root_path"] = None
        self.dbprefix = None

    def set_parser_data(self, namespace):
        "Include info from optionals which occurred up to this point."
        self.set_dbprefix(namespace.dbprefix)

    def set_dbprefix(self, dbprefix):
        assert self.dbprefix is None
        self.dbprefix = dbprefix
        real_location = self.real_location
        if self.mode == ONLINE:
            dbpath = os.path.join(
                real_location,
                pick_db_basename(real_location, self.dbprefix))
            self._treekws["dbkwargs"] = \
                {"dbpath":dbpath, "root_path":real_location}
        else:
            dbpath = real_location
            self._treekws["dbkwargs"] = {"dbpath":dbpath, "root_path":None}
    def set_dbpath(self, dbpath):
        assert self.dbprefix is not None
        self._treekws["dbkwargs"]["dbpath"] = dbpath
    def kws(self):
        """Return a dict suitable to initialize a proper Tree object,
        with dbmaker, dbkwargs, root_path."""
        assert self.dbprefix is not None
        return self._treekws

class ArgTreeOnline(ArgTree):
    def __init__(self, location):
        super().__init__(location, mandatory_mode=ONLINE)

class ArgTreeOffline(ArgTree):
    def _init__(self, location):
        super().__init__(location, mandatory_mode=OFFLINE)

# The following actions take ArgTree, update and store them.

class StoreTreeArg(argparse.Action):
    def __call__(self, parser, namespace, tree_arg, option_string=None):
        # Update a single TreeArg with parser optionals,
        # save it to self.dest
        # and also save it on a list of all locations."""
        # Optionals consumed: --exclude-once/--include-once.
        # Note: --exclude and --include are added at a final step.
        assert not isinstance(tree_arg, list)
        tree_arg.set_parser_data(namespace)
        self.save_tree_arg(namespace, tree_arg)
        # argparse autorenames exclude-once to exclude_once
        once_excludes = getattr(namespace, "exclude_once", None)
        if once_excludes is not None:
            tree_arg.exclude_patterns = list(once_excludes) # Copy.
            delattr(namespace, "exclude_once") # Reset for next tree.
        setattr(namespace, self.dest, tree_arg)
    def save_tree_arg(self, namespace, tree_arg):
        """Add this TreeArg to a full list of all locations."""
        all_tree_specs = getattr(namespace, "all_tree_specs", [])
        all_tree_specs.append(tree_arg)
        setattr(namespace, "all_tree_specs", all_tree_specs)

class StoreTreeArgList(StoreTreeArg):
    """Update and save a list of TreeArg."""
    def __call__(self, parser, namespace, tree_arg_list, option_string=None):
        assert isinstance(tree_arg_list, list)
        for tr_arg in tree_arg_list:
            tr_arg.set_parser_data(namespace)
            self.save_tree_arg(namespace, tr_arg)
        setattr(namespace, self.dest, tree_arg_list)

class StoreRoot(StoreTreeArgList):
    """Update a TreeArg and save it to self.dest, but do not save to the
    full location list."""
    def save_tree_arg(self, namespace, tree_arg):
        pass

root_option_parser = ArgumentParser(add_help=False)
root_option_parser.add_argument(
    "--root", metavar="DIR", type=ArgTreeOnline,
    action=StoreRoot, nargs="+", dest="root", default=[],
    help="read and update root database for all subtree locations")

def make_treekwargs(location, mode=ONLINE, dbprefix=DEFAULT_DBPREFIX):
    """Create a brand new treekwargs with root_path, dbmaker, dbkwargs.
    """
    tree_arg = ArgTree(location, mode)
    tree_arg.set_parser_data(argparse.Namespace(dbprefix=dbprefix))
    return tree_arg.kws()

# Options that apply to all tree locations.

def make_comparator(location):
    def comparator(section):
        try:
            if os.path.samefile(section, location):
                return True
        except:
            pass
        section = fstr(section)
        p = Pattern(fstr(section))
        return p.matches_path(location)
    return comparator

def get_cfg_location_root(location):
    "Return a ArgTreeOnline for the location or None."
    try:
        root_trees = ArgumentParser.get_from_section(
            "root",
            type=ArgTreeOnline,
            sect=make_comparator(location))
        for tr in root_trees:
            tr.set_parser_data(argparse.Namespace(dbprefix=DEFAULT_DBPREFIX))
        return root_trees
    except (NoSectionError, NoOptionError):
        return []

def get_cfg_location_excludes(location):
    excinc_objects = []
    try:
        for opt_str in ("include", "exclude"):
            try:
                excinc_objects += \
                    make_pattern_obj_list(
                        opt_str,
                        ArgumentParser.get_from_section(
                            opt_str,
                            type=fstr,
                            sect=make_comparator(location)))
            except NoOptionError:
                pass
    except NoSectionError:
        pass
    return excinc_objects

def merge_tree_patterns(tree1, tree2):
    # Make sure the exclusions are kept in sync.
    def prnt_pats(plist):
        def pre(p):
            return "--exclude" if p.is_exclude() else "--include"
        return " ".join("%s %s"% (pre(p), fstr2str(p.to_fstr())) for p in plist)
    def merge_lists(l1, l2):
        # Input lists may be altered.
        if not l1: return l2
        if not l2: return l1
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
    tr1_kws = tree1.kws()
    tr1_pats = tr1_kws["exclude_patterns"]
    tr2_kws = tree2.kws()
    tr2_pats = tr2_kws["exclude_patterns"]
    common_pats = merge_lists(list(tr1_pats), list(tr2_pats))
    if common_pats != tr1_pats or common_pats != tr2_pats:
        if any(all(all(getattr(p, f)() for p in pset)
                   for pset in (tr1_pats, tr2_pats))
               for f in ("is_include", "is_exclude")):
            outf = pr.info
        else:
            outf = pr.warning
        outf("merged rules [%s] and [%s] to [%s]" % \
            (prnt_pats(tr1_pats), prnt_pats(tr2_pats), prnt_pats(common_pats)))
    tr1_kws["exclude_patterns"] = common_pats
    tr2_kws["exclude_patterns"] = common_pats

def finish_parsing_trees(args):
    """
    Transform each tree spec into tree init kwargs, incorporating global
    --exclude and -root options, as well as exclude patterns and root options
    from the config file. Add up and apply verbosity options.
    """
    def is_subdir(path, directory):
        "Test if 'path' is a subdir of 'directory'."
        relative = os.path.relpath(path, directory)
        return not relative.startswith(fstr(os.pardir + os.sep))
    # argparse may not set default value for --exclude option.
    verbosity_delta = getattr(args, "verbosity_delta", 0)
    pr.option_verbosity += verbosity_delta
    # The excludes property is only set if exclude_option_parser was used.
    global_excludes = getattr(args, "exclude", [])
    # Need explicit 'root' default for commands without --root options,
    # e.g. cleandb.
    root_specs = getattr(args, "root", [])
    tree_specs = getattr(args, "all_tree_specs", [])
    locations_seen = []
    for tree_spec in tree_specs:
        this_location = tree_spec.real_location
        if this_location in locations_seen:
            raise ValueError(
                "duplicate location: %s" % (fstr2str(tree_spec.cmd_location,)))
        locations_seen.append(this_location)
        replacement_db = None
        if tree_spec.mode == ONLINE:
            roots_from_cfg = get_cfg_location_root(this_location)
            for root_spec in (*roots_from_cfg, *root_specs):
                root_kwargs = root_spec.kws()
                root_dir = root_kwargs["root_path"]
                if is_subdir(this_location, root_dir):
                    replacement_db = root_kwargs["dbkwargs"]["dbpath"]
                    pr.info("using %s for %s" %
                            (fstr2str(replacement_db), fstr2str(this_location)))
                    break
        if replacement_db:
            tree_spec.set_dbpath(replacement_db)
        treekwargs = tree_spec.kws() # Next, insert new data into this dict.
        once_excludes = treekwargs["exclude_patterns"]
        once_cfg_excludes = get_cfg_location_excludes(this_location)
        treekwargs["exclude_patterns"] = \
            once_cfg_excludes + once_excludes + global_excludes
        arg_to_kw = {"bysize": "size_as_hash",
                     "maxsize": "maxsize",
                     "skipempty": "skipempty"}
        for arg, treekw in arg_to_kw.items():
            # argparse argument names may not match FileHashTree _init_ kwargs:
            if hasattr(args, arg):
                assert not treekw in treekwargs, "already set: "+str(treekw)
                treekwargs[treekw] = getattr(args, arg)

# Argument check and type coerce functions:

def relative_path(value):
    """
    Argument type to exclude absolute paths.
    """
    if os.path.isabs(value):
        raise argparse.ArgumentTypeError("not a relative path: %s." % value)
    return fstr(value)

# Main parser and subcommand parsers.

# Subparsers return exit code, None meaning 0.

# Register command handler functions here for the main body:
cmd_handlers = {}  # Commands parsed fully by argparse.
cmd_handlers_extra_args = {} # Commands taking extra, non-argparse arguments.

top_parser = ArgumentParser(\
    description=DESCRIPTION,
    # The order is important here.
    parents=[configfile_option_parser, verbosity_options_parser],
    add_help=False,
    usage=argparse.SUPPRESS,
    formatter_class=lambda prog: argparse.HelpFormatter(
        prog, max_help_position=HELP_SPACING))
# The method add_subparsers allows adding alternative command via
# cmd_parsers.add_parser('cmdname', *(ArgumentParser args))
cmd_parsers = top_parser.add_subparsers(dest="cmdname", help="sub-command help")

## sync
parser_sync = cmd_parsers.add_parser(
    'sync',
    parents=[dryrun_option_parser,
             root_option_parser,
             exclude_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
            ],
    help="sync-by-rename target to best match source, no "
         "file content copied to or deleted from target")

parser_sync.add_argument(
    "source", type=ArgTree, action=StoreTreeArg)
parser_sync.add_argument(
    "targetdir", type=ArgTreeOnline, action=StoreTreeArg)

def do_sync(args):
    merge_tree_patterns(args.source, args.targetdir)
    with FileHashTree(**args.source.kws()) as src_tree:
        with FileHashTree(**args.targetdir.kws()) as tgt_tree:
            FileHashTree.scan_trees_async([src_tree, tgt_tree])
            pr.progress("matching...")
            matcher = TreePairMatcher(src_tree, tgt_tree)
            if not matcher.do_match():
                raise NotImplementedError("match failed")
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
                    raise RuntimeError(msg) from exc
            pr.progress("syncing empty dirs")
            dirs_to_rm_set = set()
            dirs_to_rm_list = []
            for dir_obj, _parent_obj, relpath \
                    in tgt_tree.walk_paths(
                            recurse=True, topdown=False,
                            dirs=True, files=False):
                if all((obj.is_dir() and obj in dirs_to_rm_set) \
                       for obj in dir_obj.entries.values()):
                    if src_tree.follow_path(relpath) is None:
                        dirs_to_rm_set.add(dir_obj)
                        dirs_to_rm_list.append(dir_obj)
            for d in dirs_to_rm_list:
                pr.print("rmdir %s" % (_quoter(fstr2str(d.get_relpath()),)))
                tgt_tree.rm_dir_writeback(d) # Obeys tgt_tree.writeback.
            pr.debug("sync done")
cmd_handlers["sync"] = do_sync

parser_rsync = cmd_parsers.add_parser(
    'rsync',
    parents=[dryrun_option_parser,
             root_option_parser,
             exclude_option_parser,
             dbprefix_option_parser,
             maxsize_option_parser
            ],
    help="generate rsync command to complete sync")

parser_rsync.add_argument(
    "-x", "--execute", default=False, action="store_true",
    help="also execute rsync command")
parser_rsync.add_argument(
    "--no-execute", default=False, action="store_false")

parser_rsync.add_argument("sourcedir", type=str)
parser_rsync.add_argument("targetdir", type=str)

def do_rsync(sysargv, args, more_args):
    """
    Print (and optionally execute) a suitable rsync command.
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
    if args.maxsize >= 0:
        rsync_opts += " --max-size=%d" % args.maxsize
    if args.dry_run:
        rsync_opts += " -n"
    cfg_excludes = get_cfg_location_excludes(fstr(args.sourcedir))
    exclude_patterns = cfg_excludes + args.exclude
    # Exclude databases at both ends.
    rsync_opts += r' --exclude="/%s[0-9]*[0-9].db"' % fstr2str(args.dbprefix)
    if exclude_patterns:
        for p in exclude_patterns:
            cmd = "exclude" if p.is_exclude() else "include"
            cmd_path = p.to_fstr()
            rsync_opts += ' --%s="%s"' % (cmd, fstr2str(cmd_path))
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
            raise RuntimeError(msg) from exc
cmd_handlers_extra_args["rsync"] = do_rsync

parser_syncr = cmd_parsers.add_parser(
    'syncr',
    parents=[dryrun_option_parser,
             root_option_parser,
             exclude_option_parser,
             dbprefix_option_parser,
             maxsize_option_parser
            ],
    help="sync and then execute the rsync command")
parser_syncr.add_argument(
    "sourcedir", type=ArgTree, action=StoreTreeArg)
parser_syncr.add_argument(
    "targetdir", type=ArgTreeOnline, action=StoreTreeArg)
def do_syncr(sysargv, args, more_args):
    args.source = args.sourcedir
    do_sync(args)
    args.sourcedir = fstr2str(args.sourcedir.real_location)
    args.targetdir = fstr2str(args.targetdir.real_location)
    args.execute = True
    do_rsync(sysargv, args, more_args)
cmd_handlers_extra_args["syncr"] = do_syncr

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
             sameline_option_parser,
             sort_option_parser],
    help='find duplicate files')
parser_fdupes.add_argument(
    "locations", type=ArgTree, action=StoreTreeArgList, nargs="+")
def do_fdupes(args):
    """
    Find duplicate files, using file size as well as file hash.
    """
    grouper = \
        GroupedFileListPrinter(args.hardlinks, args.alllinks,
                               args.sameline, args.sort)
    with FileHashTree.listof(targ.kws() for targ in args.locations) \
            as all_trees:
        FileHashTree.scan_trees_async(all_trees)
        for file_sz in fdupes.sizes_repeated(all_trees, args.hardlinks):
            with pr.ProgressPrefix("size %s:" % (bytes2human(file_sz),)):
                for _hash, located_files in \
                    fdupes.located_files_repeated_of_size(
                            all_trees, file_sz, args.hardlinks):
                    grouper.add_group(located_files)
        grouper.flush()
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
             sameline_option_parser,
             sort_option_parser],
    help='find files common to all trees')
parser_onall.add_argument(
    "locations", type=ArgTree, action=StoreTreeArgList, nargs="+")
def do_onall(args):
    if len(args.locations) == 1:
        return do_onfirstonly(args)
    grouper = \
        GroupedFileListPrinter(args.hardlinks, args.alllinks,
                               args.sameline, args.sort)
    with FileHashTree.listof(loc.kws() for loc in args.locations) as all_trees:
        FileHashTree.scan_trees_async(all_trees)
        for file_sz in sorted(fdupes.sizes_onall(all_trees)):
            with pr.ProgressPrefix("size %s:" % (bytes2human(file_sz),)):
                for _hash, located_files in \
                        fdupes.located_files_onall_of_size(all_trees, file_sz):
                    grouper.add_group(located_files)
        grouper.flush()
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
             sameline_option_parser,
             sort_option_parser],
    help='find files on first tree, not on any other')
parser_onfirstonly.add_argument(
    "locations", type=ArgTree, action=StoreTreeArgList, nargs="+")
def do_onfirstonly(args):
    grouper = \
        GroupedFileListPrinter(args.hardlinks, args.alllinks,
                               args.sameline, args.sort)
    with FileHashTree.listof(loc.kws() for loc in args.locations) as all_trees:
        FileHashTree.scan_trees_async(all_trees)
        first_tree = all_trees[0]
        other_trees = all_trees[1:]
        for file_sz in sorted(first_tree.get_all_sizes()):
            if not any(tr.size_to_files(file_sz) for tr in other_trees):
                for fobj in first_tree.size_to_files(file_sz):
                    grouper.add_group({first_tree: [fobj]})
                continue
            with pr.ProgressPrefix("size %s:" % (bytes2human(file_sz),)):
                for _hash, located_files in \
                        fdupes.located_files_onfirstonly_of_size(
                                all_trees, file_sz):
                    grouper.add_group(located_files)
        grouper.flush()
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
             sameline_option_parser,
             sort_option_parser],
    help='find files on last tree, not on any other')
parser_onlastonly.add_argument(
    "locations", type=ArgTree, action=StoreTreeArgList, nargs="+")
def do_onlastonly(args):
    locs = args.locations
    locs[0], locs[-1] = locs[-1], locs[0]
    do_onfirstonly(args)
cmd_handlers["onlastonly"] = do_onlastonly

## update
parser_update = cmd_parsers.add_parser(
    'update',
    parents=[root_option_parser,
             exclude_option_parser,
             dbprefix_option_parser,
             skipempty_option_parser,
             maxsize_option_parser
            ],
    help='update hashes of new and modified files')
parser_update.add_argument(
    "dirs", type=ArgTreeOnline, action=StoreTreeArgList, nargs="+")
def do_update(args):
    with FileHashTree.listof(d.kws() for d in args.dirs) as trees:
        for tree in trees:
            tree.db_update_all()
cmd_handlers["update"] = do_update

## rehash
parser_rehash = cmd_parsers.add_parser(
    'rehash', parents=[dbprefix_option_parser,
                       root_option_parser],
    help='force hash updates for given files')
parser_rehash.add_argument("topdir", type=ArgTreeOnline, action=StoreTreeArg)
parser_rehash.add_argument("relpaths", type=relative_path, nargs='+')
def do_rehash(args):
    return do_lookup_and_rehash(args.topdir, args.relpaths, force_rehash=True)
cmd_handlers["rehash"] = do_rehash

## lookup
parser_lookup = \
    cmd_parsers.add_parser(
        'lookup', parents=[dbprefix_option_parser,
                           root_option_parser],
        help='retrieve file hashes')
parser_lookup.add_argument("location", type=ArgTree, action=StoreTreeArg)
parser_lookup.add_argument("relpaths", type=relative_path, nargs="*")
def do_lookup(args):
    return do_lookup_and_rehash(args.location, args.relpaths, force_rehash=False)
cmd_handlers["lookup"] = do_lookup

def do_lookup_and_rehash(location_arg, relpaths, force_rehash):
    error_found = False
    with FileHashTree(**location_arg.kws()) as tree:
        for relpath in relpaths:
            file_obj = tree.follow_path(relpath)
            fname = tree.printable_path(relpath, pprint=_quoter)
            if file_obj is None or not file_obj.is_file():
                pr.error("not a file: %s" % fname)
                error_found = True
                continue
            if force_rehash:
                try:
                    tree.recompute_prop(file_obj)
                except Exception as exc:
                    pr.debug(str(exc))
                    pr.error("while rehashing %s: %s" %  (fname, str(exc)))
                    error_found = True
                    continue
            hash_val = tree.get_prop(file_obj) # Raises TreeError if no prop.
            pr.print("%d %s" % (hash_val, fname))
    return 1 if error_found else 0

## search
parser_search = cmd_parsers.add_parser(
    'search',
    parents=[root_option_parser,
             exclude_option_parser,
             hardlinks_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
             sameline_option_parser,
             sort_option_parser],
    help="Search for files by relative path glob pattern")
def pattern_fstr(arg):
    return Pattern(fstr(arg))
parser_search.add_argument(
    "locations", type=ArgTree, action=StoreTreeArgList, nargs="+")
parser_search.add_argument(
    "glob", type=pattern_fstr, action="store")
def do_search(args):
    """Search for files by relative pattern glob pattern."""
    def print_file_match(tree, fobj):
        pr.print(tree.printable_path(files_paths_matched[fobj][0]))
        for pt in files_paths_matched[fobj][1:]:
            pr.print(" ", tree.printable_path(pt))
        if args.alllinks:
            for pt in fobj.relpaths:
                if pt not in files_paths_matched[fobj]:
                    pr.print(" ", tree.printable_path(pt))
    def search_dir(tree, dir_obj, patterns):
        nonlocal files_paths_to_check
        nonlocal files_paths_matched
        if not patterns:
            return
        tree.scan_dir(dir_obj)
        patterns = set(patterns)
        for  basename, obj in dir_obj.entries.items():
            if obj.is_file():
                for p in patterns:
                    if p.matches_exactly(basename):
                        path = os.path.join(dir_obj.get_relpath(), basename)
                        if args.hardlinks or len(obj.relpaths) == 1:
                            pr.print(tree.printable_path(path))
                        else:
                            if obj not in files_paths_to_check:
                                files_paths_to_check[obj] = list(obj.relpaths)
                                files_paths_matched[obj] = []
                            assert path in files_paths_to_check[obj]
                            files_paths_to_check[obj].remove(path)
                            files_paths_matched[obj].append(path)
                            if not files_paths_to_check[obj]:
                                print_file_match(tree, obj)
                                del files_paths_to_check[obj]
                                del files_paths_matched[obj]
                        break
            if obj.is_dir():
                subdir_patterns = [p for p in patterns if not p.is_anchored()]
                for pat in patterns:
                    for tail_pat in pat.head_to_tails(basename):
                        if not tail_pat.is_empty():
                            subdir_patterns.append(tail_pat)
                if subdir_patterns:
                    search_dir(tree, obj, subdir_patterns)
    with FileHashTree.listof(treearg.kws() for treearg in args.locations) \
            as all_trees:
        for tree in all_trees:
            if not args.hardlinks:
                files_paths_to_check = {}
                files_paths_matched = {}
                tree.scan_subtree()
            search_dir(tree, tree.rootdir_obj, [args.glob])
            if not args.hardlinks:
                for fobj in files_paths_matched:
                    print_file_match(tree, fobj)
cmd_handlers["search"] = do_search

## aliases
parser_aliases = \
    cmd_parsers.add_parser(
        'aliases', parents=[dbprefix_option_parser,
                            root_option_parser],
        help='find all hardlinks to a file')
parser_aliases.add_argument("location", type=ArgTree, action=StoreTreeArg)
parser_aliases.add_argument("relpath", type=relative_path)
def do_aliases(args):
    """
    Handler for printing all alias.
    """
    with FileHashTree(**args.location.kws()) as tree:
        tree.scan_subtree() # Must scan full tree to find all aliases.
        file_obj = tree.follow_path(args.relpath)
        file_path_printable = fstr2str(args.relpath)
        if file_obj is None:
            pr.error("path does not exist: %s" % (file_path_printable,))
        elif not file_obj.is_file():
            pr.error("not a file: %s" % (file_path_printable,))
        else:
            for path in file_obj.relpaths:
                pr.print(fstr2str(path))
cmd_handlers["aliases"] = do_aliases

## cmp
parser_cmp = cmd_parsers.add_parser(
    'cmp',
    parents=[root_option_parser,
             exclude_option_parser,
             hardlinks_option_parser,
             excludeonce_option_parser,
             bysize_option_parser,
             maxsize_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser,
            ],
    help='recursively compare two trees')
parser_cmp.add_argument(
    "leftlocation", type=ArgTree, action=StoreTreeArg)
parser_cmp.add_argument(
    "rightlocation", type=ArgTree, action=StoreTreeArg)
def do_cmp(args):
    """
    Recursively compare files and dirs in two directories.
    """
    merge_tree_patterns(args.leftlocation, args.rightlocation)
    def cmp_files(path, left_obj, right_obj):
        left_prop, right_prop = None, None
        if left_obj.file_metadata.size != right_obj.file_metadata.size:
            pr.print("files differ in size: %s" % fstr2str(path,))
            return
        try:
            left_prop = left_tree.get_prop(left_obj) # TODO Threaded hashing here.
            right_prop = right_tree.get_prop(right_obj)
        except TreeError:
            if left_prop is None:
                err_path = left_tree.printable_path(path, pprint=_quoter)
            else:
                err_path = right_tree.printable_path(path, pprint=_quoter)
            pr.error("reading %s, ignoring" % (err_path,))
        else:
            if left_prop != right_prop:
                pr.print("files differ in content: %s" % fstr2str(path,))
            else:
                if args.hardlinks or \
                    len(left_obj.relpaths) == len(right_obj.relpaths) == 1:
                    pr.info("files equal: %s" % fstr2str(path,))
                else:
                    left_links = list(left_obj.relpaths)
                    right_links = list(right_obj.relpaths)
                    for ll in left_obj.relpaths:
                        if ll in right_links:
                            left_links.remove(ll)
                            right_links.remove(ll)
                    if not left_links and not right_links:
                        pr.info("files equal: %s" % fstr2str(path,))
                    else:
                        pr.print("files equal, non-matching links: %s" % \
                                 fstr2str(path,))
                        for lnk in left_links:
                            pr.print(" left only link: %s" % fstr2str(lnk,))
                        for lnk in right_links:
                            pr.print(" right only link: %s" % fstr2str(lnk,))
    def cmp_subdir(dirpaths_to_visit, cur_dirpath):
        for left_obj, basename in \
                left_tree.walk_dir_contents(cur_dirpath, dirs=True):
            left_path = os.path.join(cur_dirpath, basename)
            right_obj = right_tree.follow_path(left_path)
            if right_obj is None or right_obj.is_excluded():
                if left_obj.is_file():
                    pr.print("left only: %s" % fstr2str(left_path))
                elif left_obj.is_dir():
                    pr.print("left only: %s" %
                             fstr2str(left_path+fstr(os.path.sep)))
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
                    pr.print("left dir vs right file: %s" %
                             fstr2str(left_path))
                else:
                    pr.print("left dir vs other: %s" %
                             fstr2str(left_path+fstr(os.path.sep)))
            else:
                raise RuntimeError(
                    "unexpected left object: " + fstr2str(left_path))
        for right_obj, basename in \
                right_tree.walk_dir_contents(cur_dirpath, dirs=True):
            right_path = os.path.join(cur_dirpath, basename)
            left_obj = left_tree.follow_path(right_path)
            if left_obj is None or left_obj.is_excluded():
                if right_obj.is_file():
                    pr.print("right only: %s" % fstr2str(right_path))
                elif right_obj.is_dir():
                    pr.print("right only: %s" %
                             fstr2str(right_path+fstr(os.path.sep)))
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
    with FileHashTree(**args.leftlocation.kws()) as left_tree:
        with FileHashTree(**args.rightlocation.kws()) as right_tree:
            if not args.hardlinks:
                FileHashTree.scan_trees_async([left_tree, right_tree])
            dirpaths_to_visit = [fstr("")]
            while dirpaths_to_visit:
                cur_dirpath = dirpaths_to_visit.pop()
                cmp_subdir(dirpaths_to_visit, cur_dirpath)
cmd_handlers["cmp"] = do_cmp

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
    help='rehash and compare against stored hash')
parser_check_files.add_argument(
    "location", type=ArgTreeOnline, action=StoreTreeArg)
parser_check_files.add_argument(
    "relpaths", type=relative_path, nargs="*")

def do_check(args):
    def gen_all_paths(tr):
        for _obj, _parent, path in tr.walk_paths(
                files=True, dirs=False, recurse=True):
            yield path
    with FileHashTree(**args.location.kws()) as tree:
        assert tree.db.mode == ONLINE, "do_check tree not online"
        if not args.relpaths:
            num_items = tree.get_file_count()
            items_are_paths = False
            paths_gen = gen_all_paths(tree)
        else:
            num_items = len(args.relpaths)
            items_are_paths = True
            paths_gen = args.relpaths
        file_objs_checked_ok = set()
        file_objs_checked_bad = set()
        try:
            index = 1
            files_skipped = 0
            files_error = 0
            for path in paths_gen:
                with pr.ProgressPrefix("%d/%d:" % (index, num_items)):
                    fobj = tree.follow_path(path)
                    if fobj in file_objs_checked_ok \
                       or fobj in file_objs_checked_bad:
                        if items_are_paths:
                            index += 1
                        continue
                    try:
                        res = tree.db_check_prop(fobj)
                    except PropDBException:
                        pr.info("not checked: '%s'" % tree.printable_path(path))
                        files_skipped += 1
                        continue
                    except TreeError as exc:
                        pr.warning(
                            "while checking %s: %s" %
                            (fstr2str(path), str(exc)))
                        files_error += 1
                        continue
                    if res:
                        pr.info("passed check: %s" % tree.printable_path(path))
                        file_objs_checked_ok.add(fobj)
                    else:
                        pr.print("failed check: %s" % tree.printable_path(path))
                        file_objs_checked_bad.add(fobj)
                    index += 1
#        except KeyboardInterrupt:
#            _, v, tb = sys.exc_info()
#            pr.print("Interrupted... ")
#            raise
        finally:
            tot_files_checked = len(file_objs_checked_ok) \
                              + len(file_objs_checked_bad)
            pr.print("%d distinct file(s) checked" % (tot_files_checked,))
            tot_files_failed = len(file_objs_checked_bad)
            if files_skipped > 0:
                pr.print("%d file(s) skipped due to no existing hash" %
                         (files_skipped,))
            if files_error > 0:
                pr.print("%d file(s) skipped due to file error" %
                         (files_error,))
            if tot_files_failed > 0:
                pr.print("%d file(s) failed" % (tot_files_failed,))
                for p in file_objs_checked_bad:
                    pr.print(tree.printable_path(p.relpaths[0]))
                    if args.alllinks or args.hardlinks:
                        for other_path in p.relpaths[1:]:
                            prefix = "" if args.hardlinks else " "
                            pr.print(prefix, tree.printable_path(other_path))
                res = 1
            else:
                pr.info("no files failed check")
                res = 0
        return res
cmd_handlers["check"] = do_check

## subdir
parser_subdir = \
    cmd_parsers.add_parser(
        'subdir',
        parents=[dbprefix_option_parser],
        help='copy hashes to new database at relative subdir')
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
    with FileHashTree(**make_treekwargs(tgt_dir, ONLINE, args.dbprefix)) \
            as tgt_tree:
        tgt_tree.db_purge_old_entries()
        tgt_tree.db.compact()
cmd_handlers["subdir"] = do_subdir

## mkoffline
def writable_file_or_empty_path(path):
    if not os.path.exists(path):
        try:
            f = open(path, 'w')
        except OSError as exc:
            msg = "cannot write to %s" % (path,)
            raise argparse.ArgumentTypeError(msg) from exc
        else:
            f.close()
            os.remove(path)
    elif os.path.isfile(path):
        if not os.access(path, os.W_OK):
            msg = "cannot write to %s" % (path,)
            raise argparse.ArgumentTypeError(msg)
    else:
        msg = "not a file at %s" % (path,)
        raise argparse.ArgumentTypeError(msg)
    return fstr(path)
parser_mkoffline = cmd_parsers.add_parser(
    'mkoffline',
    parents=[exclude_option_parser,
             maxsize_option_parser,
             root_option_parser,
             skipempty_option_parser,
             dbprefix_option_parser],
    help="create offline file tree from dir")
parser_mkoffline.add_argument(
    "-f", "--force", dest="forcewrite", action="store_true", default=False,
    help="overwrite the output file, if it exists")
parser_mkoffline.add_argument(
    "sourcedir", type=ArgTreeOnline, action=StoreTreeArg)
parser_mkoffline.add_argument(
    "-o", "--outputpath", type=writable_file_or_empty_path, default=None,
    required=True)

def do_mkoffline(args):
    """
    Create an offline db by updating an online tree, copying it to
    the provided output filename and inserting file tree directory
    structure and file metadata into the outputm, offline db.
    Overwrites any file at the output.
    """
    with FileHashTree(**args.sourcedir.kws()) as src_tree:
        src_tree.db_update_all()
        if args.forcewrite and os.path.isfile(args.outputpath):
            os.remove(args.outputpath)
        with SQLPropDBManager(args.outputpath, mode=OFFLINE) as tgt_db:
            with pr.ProgressPrefix("saving: "):
                src_tree.db_store_offline(tgt_db)
            tgt_db.compact()
cmd_handlers["mkoffline"] = do_mkoffline

## cleandb
parser_cleandb = \
    cmd_parsers.add_parser(
        'cleandb',
        parents=[dbprefix_option_parser],
        help="clean and defragment the hash database at dir")
parser_cleandb.add_argument(
    "location", type=ArgTreeOnline, action=StoreTreeArg)
def do_cleandb(args):
    """Purge old entries from db and compact it.
    """
    with FileHashTree(**args.location.kws()) as tree:
        pr.progress("removing offline data")
        tree.db.rm_offline_tree()
        pr.progress("purging old entries")
        tree.db_purge_old_entries()
        pr.progress("compacting database")
        tree.db.compact()
cmd_handlers["cleandb"] = do_cleandb

def main():
    pr.set_app_prefix("lnsync: ")
    exit_error = 1
    try:
        if len(sys.argv) == 1:
            top_parser.print_help(sys.stderr)
            sys.exit(1)
        args, extra_args = top_parser.parse_known_args()
        cmd = args.cmdname
        if cmd in cmd_handlers:
            if not extra_args:
                handler_fn = lambda: cmd_handlers[cmd](args)
            else: # Let argparse explain.
                args, extra_args = top_parser.parse_args()
        elif cmd in cmd_handlers_extra_args:
            handler_fn = \
                lambda: cmd_handlers_extra_args[cmd](
                    sys.argv[1:], args, extra_args)
        else:
            assert cmd is None
            pr.error("no command")
            sys.exit()
        finish_parsing_trees(args)
        try:
            handler_exit_code = handler_fn()
        except Exception as exc:
            if __debug__:
                pr.debug("%s %s", type(exc), exc)
            raise
        if handler_exit_code is not None:
            exit_error = handler_exit_code
        else:
            exit_error = 0
    except KeyboardInterrupt:
        pr.error("interrupted")
        sys.exit(130)
    except ConfigError as exc:
        pr.error("config file: %s" % str(exc))
    except TreeError as exc:
        pr.error("file tree: %s" % str(exc))
    except NotImplementedError as exc:
        pr.error("not implemented on your system: %s", str(exc))
    except RuntimeError as exc: # Includes NotImplementedError
        pr.error("runtime: %s" % str(exc))
    except PropDBError as exc:
        pr.error("database: %s" % str(exc))
    except SQLError as exc:
        pr.error("database: %s" % str(exc))
    except AssertionError as exc:
        pr.error("internal check failed: %s" % str(exc))
    except ValueError as exc:
        pr.error("bad argument: %s" % str(exc))
    except Exception as exc:
        pr.error("general exception: %s" % str(exc))
    finally:
        sys.exit(exit_error)

if __name__ == "__main__":
    main()
