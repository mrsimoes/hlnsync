#!/usr/bin/python3

"""
Sync target file tree with source tree using hard links.

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

# pylint: disable=import-outside-toplevel, multiple-imports, invalid-name
# pylint: disable=method-hidden, redefined-builtin, broad-except

import sys
import os
import argparse
from sqlite3 import Error as SQLError
from functools import reduce

import lnsync_pkg.printutils as pr
import lnsync_pkg.metadata as metadata

from lnsync_pkg.argparse_config import \
    ConfigError, NoSectionError, NoOptionError, NoConfigFileFound, \
    ArgumentParserConfig as ArgumentParser
from lnsync_pkg.modaltype import OFFLINE
from lnsync_pkg.human2bytes import human2bytes
from lnsync_pkg.hashtree import FileHashTree, TreeError, PropDBError
from lnsync_pkg.glob_matcher import Pattern, IncludePattern, ExcludePattern
from lnsync_pkg.lnsync_treeargs import TreeLocation, TreeLocationOnline, \
    TreeLocationAction, TreeOptionAction, ConfigTreeOptionAction, \
    DEFAULT_DBPREFIX
from lnsync_pkg.blockhash import BlockHasher, HasherAlgo
import lnsync_pkg.lnsync_cmd_handlers as lnsync_cmd_handlers

####################
# Global variables and settings.
####################

TRAP_EXCEPTIONS = False # Set sys.excepthook handler.
HELP_SPACING = 30

if TRAP_EXCEPTIONS:
    def info(type, value, tb):
        if hasattr(sys, 'ps1') or not sys.stderr.isatty():
        # we are in interactive mode or we don't have a tty-like
        # device, so we call the default hook
            sys.__excepthook__(type, value, tb)
        else:
            import traceback, pdb
            # we are NOT in interactive mode, print the exception...
            traceback.print_exception(type, value, tb)
            print()
            # ...then start the debugger in post-mortem mode.
            # pdb.pm() # deprecated
            pdb.post_mortem(tb) # more "modern"
    sys.excepthook = info


def wrap(text, width):
    """
    A word-wrap function that preserves existing line breaks
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

# NB: A new version of argparse reformats the description string to fit the
# terminal width, undoing any formatting here.

####################
# Argument parsing
####################

# Utilities.

def relative_path(value):
    """
    Argument type: exclude absolute paths.
    """
    if os.path.isabs(value):
        raise argparse.ArgumentTypeError("not a relative path: %s." % value)
    return value

# Verbosity control, a top level parser.

class SetVerbosityAction(argparse.Action):
    """
    Adjust verbosity level of print module up and down.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        if "q" in option_string:
            delta = -1
        elif "v" in option_string:
            delta = 1
        else:
            raise ValueError("parsing verbosity option")
        pr.option_verbosity += delta

verbosity_options_parser = ArgumentParser(add_help=False)
verbosity_options_parser.add_argument(
    "-q", "--quiet", "-v", "--verbose",
    action=SetVerbosityAction,
    nargs=0,
    help="decrease/increase verbosity")


# Set xxhash variant (another top-level parser).

class SetXXHasher(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        BlockHasher.set_algo(getattr(HasherAlgo, values)) # Get value from name.

xxhash_option_parser = ArgumentParser(add_help=False)
xxhash_option_parser.add_argument(
    "--xxhash", choices=HasherAlgo.get_values(),
    action=SetXXHasher, default=None,
    help="set xxhash hasher variant hasher")

# Debug flag (another top-level parser).

debugtrees_option_parser = ArgumentParser(add_help=False)
debugtrees_option_parser.add_argument(
    "--debugtrees", action="store_true",
    dest="debugtrees", default=False,
    help=argparse.SUPPRESS)

# Config file choice options (another top-level parser.)

configfile_option_parser = ArgumentParser(add_help=False)

class ChooseConfigFileSet(argparse.Action):
    def __init__(self, **kw):
        try:
# Calls to ArgumentParser.set_cfg_locations must precede add_argument calls.
            ArgumentParser.set_cfg_locations(
                "./lnsync.cfg",
                os.path.expanduser("~/lnsync.cfg"),
                os.path.expanduser("~/.lnsync.cfg"))
        except NoConfigFileFound:
            pass
        super(ChooseConfigFileSet, self).__init__(**kw)
    def __call__(self, parser, namespace, value, option_string=None):
        try:
            ArgumentParser.set_cfg_locations(value)
        except ConfigError as e:
            pr.error("config file: %s" % (e))
            sys.exit(-1)
        setattr(namespace, self.dest, value)

class ChooseConfigFileUnset(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        ArgumentParser.set_cfg_locations()
        setattr(namespace, self.dest, None)

def file_expanduser(path):
    path = os.path.expanduser(path)
    return path

configfile_option_parser.add_argument(
    "--config",
    action=ChooseConfigFileSet, type=file_expanduser,
    dest="configfile", default=None,
    help="choose configuration file")

configfile_option_parser.add_argument(
    "--no-config", action=ChooseConfigFileUnset,
    nargs=0,
    dest="configfile")

####################
# Options applying to all tree arguments.
####################

# Many optional arguments are shared by multiple command parsers.
# To factor out these arguments, define as many single-argument parsers:

maxsize_option_parser = ArgumentParser(add_help=False)
maxsize_option_parser.add_argument(
    "-M", "--maxsize", type=human2bytes, default=-1,
    action=ConfigTreeOptionAction, sc_scope="all", sc_action="store",
    help="ignore files larger than MAXSIZE (default: no limit), "
         "suffixes allowed: K, M, G, etc.")

bysize_option_parser = ArgumentParser(add_help=False)
bysize_option_parser.add_argument(
    "-z", "--bysize", "--no-bysize", dest="bysize",
    action=ConfigTreeOptionAction, sc_scope="all",
    sc_action="store_true", sc_dest="size_as_hash",
    default=False,
    help="compare files by size only")

skipempty_option_parser = ArgumentParser(add_help=False)
skipempty_option_parser.add_argument(
    "-0", "--skipempty", "--no-skipempty",
    action=ConfigTreeOptionAction, sc_scope="all", sc_action="store_true",
    default=False,
    help="ignore empty files")

hard_links_option_parser = ArgumentParser(add_help=False)
hard_links_option_parser.add_argument(
    "-H", "--hard-links", "--no-hard-links",
    action=ConfigTreeOptionAction, sc_scope="all", sc_action="store_true",
    default=False,
    help="treat hard links/paths to the same file as distinct")
hard_links_option_parser.add_argument(
    "-A", "--all-links", "--no-all-links",
    action=ConfigTreeOptionAction, sc_scope="all", sc_action="store_true",
    default=False,
    help="on results, print all hard links, not just one")

##########
# Pick alternative hasher or filter functions.
##########

def valid_executable_str(path):
    """
    Exclude non-executables.
    """
    #TODO: permissions.
    import stat
    path = os.path.expanduser(path)
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError("not a file: %s" % path)
    elif not os.stat(path).st_mode & stat.S_IXUSR:
        raise argparse.ArgumentTypeError("not an executable: %s" % (path,))
    return path

class HasherExecOption(TreeOptionAction):
    @staticmethod
    def sc_action(_parser, _namespace, pos_val, _opt_val, _option_string=None):
        BlockHasher.set_algo(HasherAlgo.CUSTOM, pos_val)

hasher_option_parser = ArgumentParser(add_help=False)

hasher_option_parser.add_argument(
    "--hasher",
    type=valid_executable_str, dest="hasher_exec",
    action=TreeOptionAction,
    default=None, sc_action="store", sc_scope="all",
    help="choose hasher executable for on-line file trees")

filter_option_parser = ArgumentParser(add_help=False)

filter_option_parser.add_argument(
    "--filter",
    type=valid_executable_str, dest="filter_exec",
    action=TreeOptionAction,
    default=None, sc_action="store", sc_scope="all",
    help="choose file content filter for on-line file trees")

####################
# Options applying to one or more tree args.
####################

##########
# Database file location options
##########

dblocation_option_parser = ArgumentParser(add_help=False)

class DBPrefixTreeOption(TreeOptionAction):
    @staticmethod
    def sc_action(_parser, _namespace, pos_val, opt_val, _option_string=None):
        pos_val.set_dbprefix(opt_val)

    def sc_apply_default(self, _parser, namespace, pos_val):
        try:
            dbprefix = \
                self.get_from_tree_section(pos_val, "dbprefix", merge_sections=False)
        except ConfigError:
            dbprefix = DEFAULT_DBPREFIX
        pos_val.set_dbprefix(dbprefix)

dblocation_option_parser.add_argument(
    "-p", "--dbprefix", metavar="PREFIX", type=str,
    action=DBPrefixTreeOption, sc_scope="following",
    help="database filename prefix for following online trees")

class DBLocationTreeOption(TreeOptionAction):
    @staticmethod
    def sc_action(_parser, _namespace,
                  pos_val, opt_val, _option_string=None):
        pos_val.set_dblocation(opt_val)

    def sc_apply_default(self, _parser, namespace, pos_val):
        try:
            dblocation = \
                self.get_from_tree_section(pos_val, "dblocation",
                                           merge_sections=False)
        except ConfigError:
            pass # dblocation should not be set directly.
        else:
            pos_val.set_dblocation(dblocation)

dblocation_option_parser.add_argument(
    "-b", "--dblocation", metavar="LOCATION", type=str,
    action=DBLocationTreeOption, sc_scope="next",
    help="database file location for following online tree")

##########
# Include/exclude pattern options.
##########

class IncExcPatternOptionBase(TreeOptionAction):
    def make_pattern_obj_list(self, pattern_strings, option_string):
        """
        Transform a list of pattern strings into a list of pattern objects.
        """
        object_type = self.which_pattern_obj(option_string)
        return [object_type(p) for p in pattern_strings]

    @staticmethod
    def which_pattern_obj(option_string):
        """
        Return the correct type IncludePattern/ExcludePattern
        depending on the option string.
        """
        if "include" in option_string:
            obj_type = IncludePattern
        elif "exclude" in option_string:
            obj_type = ExcludePattern
        else:
            raise RuntimeError
        return obj_type

    def sc_apply_default(self, _parser, namespace, pos_val):
        ns = self.sc_get_namespace(pos_val)
        excinc_objects = getattr(ns, self.sc_dest, [])
        if excinc_objects is None:
            excinc_objects = []
        for opt_str in self.option_strings:
            if opt_str.startswith("--"):
                opt_str = opt_str[2:]
            elif opt_str.startswith("-"):
                opt_str = opt_str[1:]
            else:
                assert False, "unexpected option string: %s" % opt_str
            try:
                excinc_objects += \
                    self.make_pattern_obj_list(
                        self.get_from_tree_section(
                            pos_val, opt_str,
                            merge_sections=True),
                        opt_str)
            except (NoOptionError, NoSectionError):
                pass
        setattr(ns, self.sc_dest, excinc_objects)

    def __call__(self, parser, namespace, values, option_string=None):
        patterns = self.make_pattern_obj_list(values, option_string)
        super().__call__(parser, namespace, patterns, option_string)

class IncExcPatternOption(IncExcPatternOptionBase):
    pass

class IncExcOncePatternOption(IncExcPatternOptionBase):
    pass

class IncOnlyPatternOption(IncExcPatternOptionBase):
    def make_pattern_obj_list(self, pattern_strings, option_string):
        """
        Transform a list of pattern strings into a list of pattern objects.
        """
        return [IncludePattern("*/")] + \
               [IncludePattern(p) for p in pattern_strings] + \
               [ExcludePattern("*")]

exclude_option_parser = ArgumentParser(add_help=False)
exclude_option_parser.add_argument(
    "--exclude", "--include", metavar="GLOBPATTERN",
    type=str, nargs="+", dest="exclude_patterns",
    action=IncExcPatternOption, sc_scope="all", sc_action="append",
    help="exclude/include files and dirs")

excludeonce_option_parser = ArgumentParser(add_help=False)
excludeonce_option_parser.add_argument(
    "--once-exclude", "--once-include", metavar="GLOBPATTERN",
    type=str, nargs="+", dest="exclude_patterns",
    action=IncExcOncePatternOption, sc_scope="next", sc_action="append",
    help="for the following tree only")

includeonly_option_parser = ArgumentParser(add_help=False)
includeonly_option_parser.add_argument(
    "--only-include", metavar="GLOBPATTERN",
    type=str, nargs="+", dest="exclude_patterns",
    action=IncOnlyPatternOption, sc_scope="all", sc_action="append",
    help="include only the given patterns")

includeonlyonce_option_parser = ArgumentParser(add_help=False)
includeonlyonce_option_parser.add_argument(
    "--once-only-include", metavar="GLOBPATTERN",
    type=str, nargs="+", dest="exclude_patterns",
    action=IncOnlyPatternOption, sc_scope="next", sc_action="append")

exclude_all_options_parser = ArgumentParser(
    add_help=False,
    parents=[exclude_option_parser,
             excludeonce_option_parser,
             includeonly_option_parser,
             includeonlyonce_option_parser,
            ],
    )

##########
# dbrootdir option.
##########

# Specify directories where the hash database actually is.

dbrootdir_option_parser = ArgumentParser(add_help=False)

def readable_dir(path):
    if not os.path.exists(path) or not os.path.isdir(path):
        msg = "not a directory at %s" % (path,)
        raise argparse.ArgumentTypeError(msg)
    return path

class DBRootDirOption(TreeOptionAction):
    """
    Store a list of dbrootdir locations: update a TreeLocation and save it to self.dest
    but do not save to the full location list. # TODO docs
    """

    def sc_apply_default(self, _parser, namespace, pos_val):
        if pos_val.mode == OFFLINE:
            return
        try:
            dbrootdir_trees = \
                self.get_from_tree_section(pos_val,
                                           "dbrootdir",
                                           type=readable_dir,
                                           merge_sections=False)
            assert isinstance(dbrootdir_trees, list) and 0 <= len(dbrootdir_trees) <= 1
            if dbrootdir_trees:
                pos_val.set_dbrootdir_option(dbrootdir_trees[0])
        except (NoSectionError, NoOptionError):
            pass

    @staticmethod
    def sc_action(_parser, _namespace, pos_arg, opt_val, _option_string):
        assert isinstance(opt_val, list)
        for dbrootdir in opt_val:
            pos_arg.set_dbrootdir_option(dbrootdir)

dbrootdir_option_parser.add_argument(
    "--dbrootdir", metavar="DBROOTDIR",
    type=readable_dir, nargs=1, dest="dbrootdir",
    action=DBRootDirOption, sc_scope="all",
    help="database directory for all online locations")

####################
# Other shared options parsers, unrelated to trees.
####################

sameline_option_parser = ArgumentParser(add_help=False)
sameline_option_parser.add_argument(
    "-1", "--sameline", dest="sameline",
    action="store_true", default=False,
    help="print each group of identical files in the same line")
sameline_option_parser.add_argument(
    "--no-sameline", dest="sameline", action="store_false", default=False)

sort_option_parser = ArgumentParser(add_help=False)
sort_option_parser.add_argument(
    "-s", "--sort", dest="sort",
    action="store_true", default=False,
    help="sort output by size")
sort_option_parser.add_argument(
    "--no-sort", dest="sort", action="store_false", default=False)

dryrun_option_parser = ArgumentParser(add_help=False)
dryrun_option_parser.add_argument(
    "-n", "--dry-run", dest="dry_run",
    action="store_true", default=False,
    help="dry run")
dryrun_option_parser.add_argument(
    "--no-dry-run", dest="dry_run", default=False, action="store_false")

####################
# Top parser and subcommand parsers and handlers.
####################

# Command handler registry:
cmd_handlers = {}  # Commands parsed fully by argparse.
cmd_handlers_extra_args = {} # Commands taking extra, non-argparse arguments.
# Each command handler should return the final exit code, with None meaning 0.

top_parser = ArgumentParser(\
    description=DESCRIPTION,
    parents=[configfile_option_parser, # In this order.
             verbosity_options_parser,
             xxhash_option_parser,
             debugtrees_option_parser],
    add_help=False, usage=argparse.SUPPRESS,
    formatter_class=lambda prog: argparse.HelpFormatter(
        prog, max_help_position=HELP_SPACING))

cmd_parsers = top_parser.add_subparsers(dest="cmdname", help="sub-command help")

##########
# sync
##########

parser_sync = cmd_parsers.add_parser(
    'sync',
    parents=[dryrun_option_parser, exclude_option_parser,
             maxsize_option_parser,
             bysize_option_parser, skipempty_option_parser,
             dbrootdir_option_parser, dblocation_option_parser],
    help="sync-by-rename target to best match source, no "
         "file content deleted from or copied to target")
parser_sync.add_argument(
    "source", type=TreeLocation, action=TreeLocationAction)
parser_sync.add_argument(
    "target", type=TreeLocationOnline, action=TreeLocationAction)
cmd_handlers["sync"] = lnsync_cmd_handlers.do_sync

##########
# rsync
##########

parser_rsync = cmd_parsers.add_parser(
    'rsync',
    parents=[dryrun_option_parser, exclude_option_parser,
             hard_links_option_parser, maxsize_option_parser,
             dbrootdir_option_parser, dblocation_option_parser],
    help="generate an rsync command to complete sync")
parser_rsync.add_argument(
    "-x", "--execute", default=False, action="store_true",
    help="also execute rsync command")
parser_rsync.add_argument(
    "--no-execute", default=False, action="store_false")
parser_rsync.add_argument(
    "source", type=TreeLocationOnline, action=TreeLocationAction)
parser_rsync.add_argument(
    "target", type=TreeLocationOnline, action=TreeLocationAction)
cmd_handlers_extra_args["rsync"] = lnsync_cmd_handlers.do_rsync

parser_syncr = cmd_parsers.add_parser(
    'syncr',
    parents=[dryrun_option_parser, exclude_option_parser,
             hard_links_option_parser, maxsize_option_parser,
             dbrootdir_option_parser, dblocation_option_parser, ],
    help="sync and then execute the rsync command")
parser_syncr.add_argument(
    "source", type=TreeLocation, action=TreeLocationAction)
parser_syncr.add_argument(
    "target", type=TreeLocationOnline, action=TreeLocationAction)
parser_syncr.add_argument(
    "--cmp", default=False, action="store_true",
    help="compare source and target after rsync")
def do_syncr(args, more_args):
    lnsync_cmd_handlers.do_sync(args)
    args.execute = True
    lnsync_cmd_handlers.do_rsync(args, more_args)
    if args.cmp:
        args.leftlocation = args.source
        args.rightlocation = args.target
        lnsync_cmd_handlers.do_cmp(args)
cmd_handlers_extra_args["syncr"] = do_syncr

##########
# Search commands
##########

_SEARCH_CMD_PARENTS = [exclude_all_options_parser,
                       hard_links_option_parser, bysize_option_parser,
                       hasher_option_parser, maxsize_option_parser,
                       skipempty_option_parser,
                       sameline_option_parser, sort_option_parser,
                       dbrootdir_option_parser, dblocation_option_parser]

# fdupes

parser_fdupes = cmd_parsers.add_parser(
    'fdupes',
    parents=_SEARCH_CMD_PARENTS,
    help='find duplicate files')

parser_fdupes.add_argument(
    "locations", type=TreeLocation, action=TreeLocationAction, nargs="+")

cmd_handlers["fdupes"] = lnsync_cmd_handlers.do_fdupes

# onall
parser_onall = cmd_parsers.add_parser(
    'onall',
    parents=_SEARCH_CMD_PARENTS,
    help='find files common to all trees')
parser_onall.add_argument(
    "locations", type=TreeLocation, action=TreeLocationAction, nargs="+")
cmd_handlers["onall"] = lnsync_cmd_handlers.do_onall

# onfirstonly

parser_onfirstonly = cmd_parsers.add_parser(
    'onfirstonly',
    parents=_SEARCH_CMD_PARENTS,
    help='find files on first tree, not on any other')
parser_onfirstonly.add_argument(
    "locations", type=TreeLocation, action=TreeLocationAction, nargs="+")
cmd_handlers["onfirstonly"] = lnsync_cmd_handlers.do_onfirstonly

# onlastonly

parser_onlastonly = cmd_parsers.add_parser(
    'onlastonly',
    parents=_SEARCH_CMD_PARENTS,
    help='find files on last tree, not on any other')
parser_onlastonly.add_argument(
    "locations", type=TreeLocation, action=TreeLocationAction, nargs="+")
cmd_handlers["onlastonly"] = lnsync_cmd_handlers.do_onlastonly

# search

parser_search = cmd_parsers.add_parser(
    'search',
    parents=_SEARCH_CMD_PARENTS,
    help="Search for files by relative path glob pattern")
parser_search.add_argument(
    "locations", type=TreeLocation, action=TreeLocationAction, nargs="+")
parser_search.add_argument("glob", type=Pattern, action="store")
cmd_handlers["search"] = lnsync_cmd_handlers.do_search

##########
# update
##########

parser_update = cmd_parsers.add_parser(
    'update',
    parents=[exclude_all_options_parser,
             skipempty_option_parser, maxsize_option_parser,
             dbrootdir_option_parser, dblocation_option_parser],
    help='update hashes of new and modified files')
parser_update.add_argument(
    "dirs", type=TreeLocationOnline, action=TreeLocationAction, nargs="+")
def do_update(args):
    with FileHashTree.listof(d.kws() for d in args.dirs) as trees:
        for tree in trees:
            tree.db_update_all()
cmd_handlers["update"] = do_update

##########
# rehash
##########

parser_rehash = cmd_parsers.add_parser(
    'rehash', parents=[dbrootdir_option_parser, dblocation_option_parser],
    help='force hash updates for given files')
parser_rehash.add_argument("topdir",
                           type=TreeLocationOnline, action=TreeLocationAction)
parser_rehash.add_argument("relpaths", type=relative_path, nargs='+')
def do_rehash(args):
    return lnsync_cmd_handlers.do_lookup_and_rehash( \
                args.topdir, args.relpaths, force_rehash=True)
cmd_handlers["rehash"] = do_rehash

##########
# lookup
##########

parser_lookup = \
    cmd_parsers.add_parser(
        'lookup',
        parents=[dbrootdir_option_parser, dblocation_option_parser],
        help='retrieve file hashes')
parser_lookup.add_argument("location",
                           type=TreeLocation, action=TreeLocationAction)
parser_lookup.add_argument("relpaths", type=relative_path, nargs="*")
def do_lookup(args):
    return lnsync_cmd_handlers.do_lookup_and_rehash( \
                args.location, args.relpaths, force_rehash=False)
cmd_handlers["lookup"] = do_lookup

##########
# aliases
##########

parser_aliases = \
    cmd_parsers.add_parser(
        'aliases',
        parents=[dbrootdir_option_parser, dblocation_option_parser],
        help='find all hard links to a file')
parser_aliases.add_argument("location",
                            type=TreeLocation, action=TreeLocationAction)
parser_aliases.add_argument("relpath", type=relative_path)
cmd_handlers["aliases"] = lnsync_cmd_handlers.do_aliases

##########
# cmp
##########

parser_cmp = cmd_parsers.add_parser(
    'cmp',
    parents=[exclude_all_options_parser,
             hard_links_option_parser, bysize_option_parser,
             maxsize_option_parser, skipempty_option_parser,
             dbrootdir_option_parser, dblocation_option_parser,
            ],
    help='recursively compare two trees')
parser_cmp.add_argument(
    "leftlocation", type=TreeLocation, action=TreeLocationAction)
parser_cmp.add_argument(
    "rightlocation", type=TreeLocation, action=TreeLocationAction)
cmd_handlers["cmp"] = lnsync_cmd_handlers.do_cmp

##########
# check
##########

parser_check_files = cmd_parsers.add_parser(
    'check',
    parents=[exclude_all_options_parser,
             hard_links_option_parser, bysize_option_parser,
             maxsize_option_parser, skipempty_option_parser,
             dbrootdir_option_parser, dblocation_option_parser],
    help='rehash and compare against stored hash')

parser_check_files.add_argument(
    "location", type=TreeLocationOnline, action=TreeLocationAction)

parser_check_files.add_argument(
    "relpaths", type=relative_path, nargs="*")

cmd_handlers["check"] = lnsync_cmd_handlers.do_check

##########
# subdir
##########

parser_subdir = \
    cmd_parsers.add_parser(
        'subdir',
        parents=[dblocation_option_parser],
        help='copy hashes to new database at relative subdir')

parser_subdir.add_argument("topdir",
                           type=TreeLocationOnline, action=TreeLocationAction)

parser_subdir.add_argument("relativesubdir", type=relative_path)

cmd_handlers["subdir"] = lnsync_cmd_handlers.do_subdir

##########
# mkoffline
##########

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
    return path

parser_mkoffline = cmd_parsers.add_parser(
    'mkoffline',
    parents=[exclude_all_options_parser, maxsize_option_parser,
             skipempty_option_parser,
             dbrootdir_option_parser, dblocation_option_parser],
    help="create offline file tree from dir")

parser_mkoffline.add_argument(
    "-f", "--force", dest="forcewrite", action="store_true", default=False,
    help="overwrite the output file, if it exists")

parser_mkoffline.add_argument(
    "sourcedir", type=TreeLocationOnline, action=TreeLocationAction)

parser_mkoffline.add_argument(
    "-o", "--outputpath", type=writable_file_or_empty_path, default=None,
    required=True)

cmd_handlers["mkoffline"] = lnsync_cmd_handlers.do_mkoffline

##########
## cleandb
##########

parser_cleandb = \
    cmd_parsers.add_parser(
        'cleandb',
        parents=[dblocation_option_parser],
        help="clean and defragment the hash database at dir")

parser_cleandb.add_argument(
    "location", type=TreeLocationOnline, action=TreeLocationAction)

cmd_handlers["cleandb"] = lnsync_cmd_handlers.do_cleandb

####################
# main
####################

def debug_tree_info(args):
    xargs = vars(args)
    def excstr(tr):
        if not hasattr(tr, "dbprefix"):
            tr.kws = "No kws, path=" + tr.real_location
        elif callable(tr.kws):
            tr.kws = tr.kws()
        if "exclude_patterns" in tr.kws:
            tr.kws["exclude_patterns"] = \
                list(map(str, tr.kws["exclude_patterns"]))
    for t in xargs:
        if isinstance(xargs[t], TreeLocation):
            excstr(xargs[t])
            print("%s -> %s\n" % (t, xargs[t].kws))
        elif isinstance(xargs[t], list) \
                and all(isinstance(x, TreeLocation) for x in xargs[t]):
            print("%s -> [" % (t))
            for k, _x in enumerate(xargs[t]):
                excstr(xargs[t][k])
                print(" [%s] -> %s" % (k, xargs[t][k].kws))
            print("    ]\n")

def get_handler_fn():
    try:
        args, extra_args = top_parser.parse_known_args()
        cmd = args.cmdname
    except ConfigError as exc:
        pr.error("config file: %s" % str(exc))
        sys.exit(1)
    except ValueError as exc:
        pr.error("bad argument: %s" % str(exc))
        sys.exit(1)
    if cmd in cmd_handlers:
        if not extra_args:
            handler_fn = lambda: cmd_handlers[cmd](args)
        else: # Let this fail and argparse explain why.
            args, extra_args = top_parser.parse_args()
    elif cmd in cmd_handlers_extra_args:
        # Extra args only allwoed at the end
        if extra_args and extra_args != sys.argv[-len(extra_args):]:
            top_parser.parse_args()
            assert False, "internal error"
        handler_fn = \
            lambda: cmd_handlers_extra_args[cmd](args, extra_args)
    else:
        assert cmd is None
        pr.error("no command")
        sys.exit()
    if args.debugtrees:
        debug_tree_info(args)
        sys.exit(-1)
    return handler_fn

def main():
    if len(sys.argv) == 1:
        top_parser.print_help(sys.stderr)
        sys.exit(1)
    pr.set_app_prefix("lnsync: ")
    cmd_handler = get_handler_fn()
    try:
        exit_error = 1
        exit_error = cmd_handler()
        if exit_error is None:
            exit_error = 0
    except KeyboardInterrupt:
        pr.error("interrupted")
        sys.exit(130)
    except TreeError as exc:
        pr.error("file tree: %s" % str(exc))
    except (PropDBError, SQLError) as exc:
        pr.error("database: %s" % str(exc))
    except NotImplementedError as exc:
        pr.error("not implemented on your system: %s", str(exc))
    except (RuntimeError, AssertionError, Exception) as exc:
        pr.error("internal error: %s" % str(exc))
    finally:
        sys.exit(exit_error)

if __name__ == "__main__":
    main()
