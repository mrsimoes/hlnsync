#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
Overview:

Each command accepts one or more tree positional arguments (directories and/or
offline tree files), as well as optional arguments. ('positional' and 'optional'
in the argparse.)

Some optional arguments are tree optional arguments.

Tree optional arguments come in scoped varieties, in that they apply to the next
tree in the command line, or to all subsequent trees in the command line, or to
all trees in the command line. This is achieved using the
argparse_scoped_options module.

Tree positional arguments make use of both argparse type and argparse action.

- The type TreeLocation (and its subtypes TreeLocationOnline and
TreeLocationOffline) gathers information (partial, incomplete, provisional)
that will be later used to create the actual Tree object.

- The action (TreeLocationAction) records the tree positional arguments found
and applies the options found so far. There is also an Action to process a list
of trees for arguments with nargs='+'.

Optional tree arguments use this arparse Action:

- TreeOptionAction registers that option in the parser namespace and also, if
the scope of this optional argument is global, it is applied the all previous
tree arguments. (The subtype ConfigTreeOptionAction automatically searches the
config file for values for the option.)

"""

# pylint: disable=no-member, redefined-builtin, unused-import

import os
import argparse

import lnsync_pkg.printutils as pr
from lnsync_pkg.glob_matcher import Pattern, ExcludePattern, merge_pattern_lists
from lnsync_pkg.miscutils import is_subdir, append_to_namespace_list
from lnsync_pkg.modaltype import Mode
from lnsync_pkg.argparse_scoped import ScOptArgAction, ScPosArgAction, Scope
from lnsync_pkg.prefixdbname import mode_from_location, pick_db_basename
from lnsync_pkg.argparse_config import NoSectionError, NoOptionError, \
    ArgumentParserConfig, ConfigError

class TreeLocationAction(ScPosArgAction):
    def __call__(self, parser, namespace, val, option_string=None):
        super().__call__(parser, namespace, val, option_string)
        tree_args = getattr(namespace, "sc_pos_args", [])
        locations_seen = []
        for tree_arg in tree_args:
            this_location = tree_arg.real_location
            if this_location in locations_seen:
                raise ValueError("duplicate location: " + this_location)
            else:
                locations_seen.append(this_location)

#class TreeLocationListAction(ScPosArgListAction):
#    pass

class TreeLocation:
    """
    Gather, mostly from the command-line, data required to create tree location
    objects. Create the necessary keyword arguments to init those objects.

    Notable attributes:
    - cmd_location: the tree location, exactly as specified in the command line,
    either a directory in online mode or a database file in offline mode).
    - real_path (the canonical form of cmd_location, via os.path.realpath).

    The dblocation attribute is only be set when a database prefix, or a
    specific alternative database location, is provided via the set_dbprefix
    or set_dblocation methods. (An explicit alternative location takes
    precedence over a db prefix.)

    Attributes that will result in Tree object init keywords are stored
    in the namespace attribute.
    """

    def __new__(cls, location, mandatory_mode=Mode.NONE):
        """
        Make it equivalent to create instances with TreeLocationOnline(loc),
        TreeLocation(loc, mode=ONLINE), even TreeLocation(loc) so long as loc is
        a dir.
        """
        mode = mode_from_location(location, mandatory_mode)
        assert mode in (Mode.ONLINE, Mode.OFFLINE), \
            "__new__: mode must be ONLINE or OFFLINE"
        newcls = {Mode.ONLINE: TreeLocationOnline,
                  Mode.OFFLINE: TreeLocationOffline}[mode]
        newobj = super().__new__(newcls)
        newobj.mode = mode
        if not isinstance(newobj, cls):
            newobj.__init__(location)
        return newobj

    def __init__(self, location):
        self.cmd_location = location
        self.real_location = os.path.realpath(self.cmd_location)
        self.namespace = argparse.Namespace()
        setattr(self.namespace, "mode", self.mode)

    def kws(self):
        """
        Return a dict suitable to initialize a proper Tree object,
        with at least mode, dbmaker, dbkwargs, topdir_path.
        """
        return vars(self.namespace)

    @staticmethod
    def merge_patterns(tree1, tree2):
        """
        Merge the exclude/include patterns of tree1 and tree2 and update them
        both.
        """
        tr1_pats = getattr(tree1.namespace, "exclude_patterns")
        tr2_pats = getattr(tree2.namespace, "exclude_patterns")
        merged_pats = merge_pattern_lists(tr1_pats, tr2_pats)
        setattr(tree1.namespace, "exclude_patterns", merged_pats)
        setattr(tree2.namespace, "exclude_patterns", merged_pats)


class TreeLocationOffline(TreeLocation):
    def __init__(self, cmd_location):
        super().__init__(cmd_location)
        setattr(self.namespace, "exclude_patterns", [])
        setattr(self.namespace, "topdir_path", None)
        setattr(self.namespace, "dbkwargs", \
                {"dbpath":self.cmd_location, "topdir_path":None})

# The following have no effect on offline trees:
    def set_dbprefix(self, _dbprefix):
        pass

    def set_dblocation(self, _dbpath):
        pass

    def set_alt_dbrootdir(self, alt_dbrootdir):
        pass

    def set_alt_dbrootdir_parent(self, alt_dbrootdir):
        pass

class TreeLocationOnline(TreeLocation):
    def __init__(self, cmd_location):
        super().__init__(cmd_location)
        self._kws = None
        self.dblocation = None
        self._dbprefix = None
        setattr(self.namespace, "topdir_path", self.cmd_location)
        self._alt_dbrootdir = None

    def set_dbprefix(self, dbprefix):
        assert self._kws is None, \
            "TLO: cannot set dbprefix after kwargs generated"
        self._dbprefix = dbprefix
        append_to_namespace_list(
            self.namespace,
            "exclude_patterns",
            [ExcludePattern(f"/{dbprefix}-*.db"),])

    def get_dbprefix(self):
        assert self._dbprefix is not None, \
            "TLO: dbprefix not set"
        return self._dbprefix

    def set_dblocation(self, dblocation):
        if getattr(self, "dblocation"):
            msg = f"tree at '{self.real_location}': " \
                  f"db location already set to '{self.dblocation}' " \
                  f"while trying to set it at '{dblocation}"
            raise ConfigError(msg)
        assert self._kws is None, \
            "TLO: cannot set db;location after kwargs generated"
        assert isinstance(dblocation, str), \
            "TLO: dbpath must be a string"
        if os.path.exists(dblocation) and not os.path.isfile(dblocation):
            raise ValueError("not a file: " + dblocation)
        dblocation = os.path.realpath(dblocation)
        self.dblocation = dblocation
        dbdirpath = os.path.dirname(dblocation)
        relpath = \
            is_subdir(dbdirpath, self.real_location)
        if relpath:
            if os.path.samefile(dbdirpath, self.real_location):
                relpath = ""
            relpath = os.path.join(relpath, os.path.basename(dblocation))
            setattr(self.namespace,
                    "exclude_patterns",
                    [ExcludePattern(f"/{relpath}"),])

    def set_alt_dbrootdir(self, alt_dbrootdir):
        """
        Assume alt_dbrootdir is a readable directory.
        If it is a superdir of the real location of this offline tree,
        set it as our dbrootdir.
        Prefer more specific dbrootdirs if multiple calls are made.
        """
        if not is_subdir(self.real_location, alt_dbrootdir):
#                or os.path.samefile(self.real_location, alt_dbrootdir):
            pr.trace("dbrootdir does not apply: %s for %s",
                     alt_dbrootdir, self.real_location)
            return
        if self._alt_dbrootdir is not None \
               and is_subdir(self._alt_dbrootdir, alt_dbrootdir):
            pr.trace("dbrootdir less specific: %s for %s",
                     alt_dbrootdir, self._alt_dbrootdir)
            return
        self._alt_dbrootdir = alt_dbrootdir

    def set_alt_dbrootdir_parent(self, alt_dbrootdir_parent):
        """
        If real_location is a subdir of alt_dbrootdir_parent,
        set alt_dbrootdir to the subdir of alt_dbrootdir_parent
        that contains real_location.
        This is used with alt_dbrootdir_parent as the directory
        containing all mountpoints of removable media.
        """
        if not is_subdir(self.real_location, alt_dbrootdir_parent) \
           or os.path.samefile(self.real_location, alt_dbrootdir_parent):
            pr.trace("dbrootdir_parent does not apply: %s for %s",
                     alt_dbrootdir_parent, self.real_location)
            return
        relpath = os.path.relpath(self.real_location, alt_dbrootdir_parent)
        subdir = relpath.split(os.sep)[0]
        self.set_alt_dbrootdir(os.path.join(alt_dbrootdir_parent, subdir))

    def compute_dbdir(self):
        assert self._dbprefix is not None, "TLO: missing dbprefix"
        if self._alt_dbrootdir is not None:
            db_dir = self._alt_dbrootdir
        else:
            db_dir = self.cmd_location
        return db_dir

    def kws(self):
        if self._kws is None:
            if self.dblocation is None:
                db_dir = self.compute_dbdir()
                db_basename = pick_db_basename(db_dir, self._dbprefix)
                dblocation = os.path.join(db_dir, db_basename)
                pr.info("using %s for %s" % (dblocation, self.cmd_location))
            else:
                dblocation = self.dblocation
            setattr(
                self.namespace, "dbkwargs",
                {"dbpath":dblocation, "topdir_path":self.cmd_location})
            self._kws = super().kws()
        return self._kws


class TreeOptionAction(ScOptArgAction):
    def sc_get_namespace(self, pos_val):
        return pos_val.namespace

    @staticmethod
    def make_comparator(location):
        """
        Makes a comparator function that will match config file section
        wildcards to the given dir location.
        """
        def comparator(section, location=location):
            try:
                if os.path.samefile(section, location):
                    return True
            except OSError:
                pass
            pat = Pattern(section)
            return pat.matches_path(location)
        return comparator

    @staticmethod
    def is_config_file_enabled():
        return ArgumentParserConfig.is_active()

    def get_from_tree_section(self, arg_tree, key, merge_sections, type=None):
        """
        Return value corresponding to key from locations matching arg_tree's
        real location.
        """
        if type is None:
            type = self.type
        location = arg_tree.real_location
        section_name_comparator = TreeOptionAction.make_comparator(location)
        val = ArgumentParserConfig.get_from_section(
            key,
            type=type,
            section=section_name_comparator,
            merge_sections=merge_sections,
            nargs=self.nargs)
        return val

class ConfigTreeOptionAction(TreeOptionAction):
    """
    A TreeOptionAction that reads default values from the config file sections
    matching this tree.
    """
    def sc_apply_default(self, parser, namespace, pos_val):
        super().sc_apply_default(parser, namespace, pos_val)
        if not self.is_config_file_enabled():
            return
        for opt_str in self.option_strings:
            if opt_str.startswith("--"):
                short_opt_str = opt_str[2:]
            elif opt_str.startswith("-"):
                short_opt_str = opt_str[1:]
            else:
                assert False, "unexpected option string: %s" % opt_str
            try:
                vals = self.get_from_tree_section(
                    pos_val,
                    short_opt_str,
                    type=self.type,
                    merge_sections=True)
            except (NoSectionError, NoOptionError):
                continue
            try:
                self.sc_action(
                    parser, namespace, pos_val, vals, opt_str)
            except Exception as exc:
                exc_type = type(exc)
                msg = f"while applying config {opt_str}: {str(exc)}"
                raise exc_type(msg) from exc
