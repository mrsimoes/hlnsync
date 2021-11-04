# Copyright (C) 2020 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
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
Extend argparse to read non-positional arguments from a config file.

Usage:
Optional argument values are read from the DEFAULT section of an ini-style file
using configparser.

The configfile key is any of the admissible option strings.

The value may either be empty, a single line non-empty line, or multiple values
in multiple lines.

For switches without arguments, if a single integer is given, the option is
repeated so many times.

Methods are provided to read sections other than the DEFAULT section.

The configuration files may only be chosen module-wide, which allows specifying
the config files before creating ArgumentParserConfig and subparser instances,
which is required so that subparser instances can read default values at
__init__ time.
"""

# pylint: disable=redefined-builtin

import os
import sys
import argparse
import configparser
from functools import reduce
from enum import Enum

import lnsync_pkg.printutils as pr

def file_expanduser(path):
    path = os.path.expanduser(path)
    return path

ConfigError = configparser.Error
NoSectionError = configparser.NoSectionError
NoOptionError = configparser.NoOptionError
class NoConfigFileSet(ConfigError):
    pass
class NoValidConfigFile(configparser.Error):
    pass
class NoUniqueSectionError(configparser.Error):
    pass
class WrongValueCountError(configparser.Error):
    pass

class CLIConfig(Enum):
    NO_CHOICE = 0
    SET_NO_CONFIG_FILE = 1
    SET_CONFIG_FILE_UNREAD = 2
    SET_CONFIG_FILE_READ = 3

class ArgumentParserConfig(argparse.ArgumentParser):
    """
    Drop-in replacement for argparse.ArgumentParser that reads argument
    values from a config file.

    Calls to add_argument are intercepted
    """

    _cfg_mgr = None
    _default_config_files = []

# Class methods:
# Setting the config file locations, fetching.

    @classmethod
    def is_enabled(cls):
        return cls._cfg_mgr is not None

    @classmethod
    def  set_default_config_files(cls, *filenames):
        cls._default_config_files = filenames

    @classmethod
    def _read_config_files(cls, *filenames):
        """
        Read the first file that exists, disregard the following.
        Raise exception if there is an error on first that exists,
        or if none exists.
        """
        def hyphens_to_underscores(sect_name):
            return sect_name.replace("-", "_")
        cfg_parser = configparser.ConfigParser()
        cfg_parser.optionxform = hyphens_to_underscores
        ArgumentParserConfig._cfg_mgr = cfg_parser
        for fname in filenames:
            if os.path.exists(fname):
                try:
                    read_files = ArgumentParserConfig._cfg_mgr.read(fname)
                except configparser.ParsingError as exc:
                    raise ConfigError(str(exc)) from exc
                if read_files:
                    return
        raise NoValidConfigFile("trying to read: %s" % (",".join(filenames),))

    @classmethod
    def get_from_default(cls, key, type=None, nargs=None):
        vals = cls.get_from_section(key, sect=configparser.DEFAULTSECT,
                                    type=type, nargs=nargs)
        return vals

    @classmethod
    def get_from_section(cls, key, sect, type=None,
                         merge_sections=True, nargs=None):
        """
        Return a list of values corresponding to key from section sect,
        where sect is either the literal section name or a callable predicate.
        If no key is present in any matching section, try the DEFAULT section.

        If multiple sections match and hold values for the key, either raise
        an exception or merge the results, depending on merge_sections.

        If merge_sections is True, include values from the DEFAULT section.
        If merge_sections, search fetch from all matching sections. If this is
        false and multiple sections match, raise NoUniqueSectionError.
        """
        if cls._cfg_mgr is None:
            raise NoConfigFileSet
        values_lists = cls._get_values_from_section(key, sect, type)
        if not values_lists:
            msg = "no section found while fetching key %s" % (key,)
            raise NoSectionError(msg)
        values = cls._process_values_nargs(
            values_lists, key, nargs, merge_sections)
        return values

    @classmethod
    def _get_values_from_section(cls, key, sect, type=None):
        """
        Fetch key values strings from each matching section, apply type to each,
        and return a list of those value lists.
        If no section matches sect, look up key in the default section.
        """
        def process_raw_valuestr(valuestr):
            """
            Return a list of typed values corresponding to the raw config value
            string. Split the config value by line breaks and, if a type was
            provided, apply it to each line.
            """
            substrings = valuestr.split("\n")
            split_raw = filter(lambda v: len(v) > 0, substrings)
            if type is not None:
                split_raw = map(type, split_raw)
            res = list(split_raw)
            return res
        mgr = cls._cfg_mgr
        if not callable(sect):
            values = process_raw_valuestr(mgr.get(sect, key))
            values = [values]
        else:
            matching_sections = [s for s in mgr.sections() if sect(s)]
            if not matching_sections:
                try:
                    values = \
                        cls._get_values_from_section(
                            key, sect=configparser.DEFAULTSECT,
                            type=type)
                except ConfigError:
                    values = []
            else:
                values = []
                for s in matching_sections:
                    try:
                        values.append(process_raw_valuestr(mgr.get(s, key)))
                    except NoOptionError:
                        pass
        return values

    @classmethod
    def _process_values_nargs(cls, values_lists, key, nargs, merge_sections):
        """
        Given a list of lists of values, pick the correct value or values
        depending on nargs and merge_sections.
        """
        def check_value_count(values_lists, legal_counts):
            for values in values_lists:
                if len(values) not in legal_counts:
                    msg = "%s: expected %s argument(s), got %s" % \
                             (key, legal_counts, values)
                    raise WrongValueCountError(msg)
        def to_values(values_lists):
            if merge_sections:
                values = reduce(list.__add__, values_lists)
            else:
                if len(values_lists) > 1:
                    msg = "%s: multiple sections" % (key,)
                    raise NoUniqueSectionError(msg)
                else:
                    values = values_lists[0]
            return values
        if nargs is None:
            # argparse returns a value for nargs==None
            # and a one-elem list for nargs==1.
            # We do the same.
            check_value_count(values_lists, legal_counts=(0, 1))
            values = to_values(values_lists)
            values = values[-1]
        elif nargs == 0:
            # No arguments expected, read repeat count.
            check_value_count(values_lists, legal_counts=(0,))
            values = []
        elif isinstance(nargs, int):
            check_value_count(values_lists, legal_counts=(nargs,))
            values = values_lists[-1]
        elif nargs in ("+", "*"):
            values = to_values(values_lists)
        else:
            assert False, \
                "unexpected nargs: %s" % (nargs,)
        return values

    # Instance methods.

    def __init__(self, *args, **kwargs):
        parents = kwargs.pop("parents", [])
        configfile_option_parser = argparse.ArgumentParser(add_help=False)
        self.configfile_option_parser = configfile_option_parser
        configfile_option_parser.add_argument(
            "--config", type=file_expanduser,
            action=ChooseConfigFileSet, dest="configfile",
            help="choose configuration file")
        configfile_option_parser.add_argument(
            "--no-config", nargs=0,
            action=ChooseConfigFileUnset, dest="configfile")
        parents = [configfile_option_parser] + parents
        super().__init__(args, parents=parents, **kwargs)

    def add_subparsers(self, *args, **kwargs):
        return super().add_subparsers(
            *args,
            parser_class=argparse.ArgumentParser, **kwargs)

    def parse_known_args(self, args=None, namespace=None):
        """
        Go through the parser actions and run each whose option string matches
        an entry in the config file default section.
        """
        def switch_match(cli_arg, min_switch, full_switch):
            return cli_arg.startswith(min_switch) \
                   and full_switch.startswith(cli_arg)
        if namespace is None:
            namespace = argparse.Namespace()
#        configfile_option_parser = self.configfile_option_parser
        if switch_match(sys.argv[1], "--no-conf", "--no-config"):
            fo_val = CLIConfig.SET_NO_CONFIG_FILE
        elif switch_match(sys.argv[1], "--conf", "-config"):
            fo_val = CLIConfig.SET_CONFIG_FILE_UNREAD
            self._read_config_files(file_expanduser(sys.argv[2]))
        else:
            fo_val = CLIConfig.NO_CHOICE
            self._read_config_files(*self._default_config_files)
        if ArgumentParserConfig._cfg_mgr is not None:
            self._config_file_exec_all_actions(namespace)
#        assert not hasattr(namespace, "_config_file_option"), \
#            "parse_known_args: unexpected attribute clash in namespace"
#        setattr(namespace, "_config_file_option", fo_val)
        ChooseConfigAction._config_file_option = fo_val
        return super().parse_known_args(args, namespace)

    def _config_file_exec_all_actions(self, namespace):
        assert ArgumentParserConfig._cfg_mgr is not None, \
            "_config_file_exec_all_actions: no _cfg_mgr"
        for action in self._actions:
            if isinstance(action, (ChooseConfigFileSet, ChooseConfigFileUnset)):
                continue
            if hasattr(action, "option_strings") \
                    and action.option_strings \
                    and action.option_strings[0] == "-h": # Skip Help Actions.
                continue
            if hasattr(action, "default"):
                setattr(namespace, action.dest, getattr(action, "default"))
            self._config_file_exec_one_action(action, namespace)

    def _config_file_exec_one_action(self, action, namespace,
                                     option_strings=None):
        """
        Call this action with the values from the default section of the
        config file (minding nargs), as if the config entry had been input from
        the command line.
        Raise various ConfigError if there are too many or not enough arguments.
        """
        assert ArgumentParserConfig._cfg_mgr is not None, \
            "_config_file_exec_one_action: no _cfg_mgr"

        if option_strings is None:
            option_strings = action.option_strings

        if not option_strings \
                or option_strings[0][0] != "-" \
                or option_strings[0] == "-h":
            return

        for option_string in option_strings:
            assert option_string[0] == "-", \
                f"_config_file_exec_one_action: invalid: {option_string}"
            option_string = option_string.lstrip("-").replace("-", "_")
            try:
                values = ArgumentParserConfig.get_from_default(
                    key=option_string, type=action.type,
                    nargs=action.nargs)
            except ConfigError:
                continue
            action(self, namespace, values, option_string)
            break

# These actions are meant to be used before any other action,
# to select the config files.
# The, during regular parsing, they should do nothing.

class ChooseConfigAction(argparse.Action):
    def _inconsistent_usage(self):
        raise argparse.ArgumentError(
            self, '--no-config and --config are mutually exclusive')
    def _config_options_first(self):
        raise argparse.ArgumentError(
            self,
            '--no-config and --config must be first on the command line')
    def __call__(self, parser, namespace, value, option_string=None):
        if self._config_file_option == CLIConfig.NO_CHOICE:
            self._config_options_first()

class ChooseConfigFileSet(ChooseConfigAction):
    def __call__(self, parser, namespace, value, option_string=None):
        super().__call__(parser, namespace, value, option_string)
        if self._config_file_option == CLIConfig.SET_NO_CONFIG_FILE:
            self._inconsistent_usage()
        elif self._config_file_option == CLIConfig.SET_CONFIG_FILE_READ:
            self._config_options_first()
        assert self._config_file_option == CLIConfig.SET_CONFIG_FILE_UNREAD
        try:
            ArgumentParserConfig.set_default_config_files(value)
        except ConfigError as e:
            pr.error("config file: %s" % (e))
            sys.exit(1)
        ChooseConfigAction._config_file_option = CLIConfig.SET_CONFIG_FILE_READ

class ChooseConfigFileUnset(ChooseConfigAction):
    def __call__(self, parser, namespace, value, option_string=None):
        super().__call__(parser, namespace, value, option_string)
        if self._config_file_option in \
                (CLIConfig.SET_CONFIG_FILE_UNREAD, CLIConfig.SET_CONFIG_FILE_READ):
            self._inconsistent_usage()
