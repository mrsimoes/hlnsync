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
This extends argparse ArgumentParser to read non-positional arguments and their
values values from an ini-style config file, using the configparser module.

It's a big kludge, but maybe a useful one.

TODO
INI-style files are used just because the configparser module was handy.
Major drawbacks:
    - Entries cannot be read in order.
    - No multiple entries for the same key in a section.

Usage:
------
Create your parser as an instance of ArgumentParserConfig, instead of
argparse.ArgumentParser.

If a custom parser_class is needed in add_subparsers, make sure it derives
from ArgumentParserConfigSubparser.

Optional argument values are collected from a specific ini-section in the
config file. The default section is OPTIONALS, but this is configurable: 
    ArgumentParserConfig.set_optionals_section(section_name):

Before parsing, for each key in the optionals section matching an option string
of an optinal argumnt (minus the leading dashes), the values are read, cast
into the appropriate type and fed in to the corresponding argparse.Action.
Multiple values to an optional argument are given in separate lines.
An empty ini-file value corresponds to no optional argument value.

While parsing, the option --config-section <SECTIONNAME> reads options as
before, but from the given SECTION.

The configuration file locations may either be set before creating the argument
parser instances:
    ArgumentParserConfig.set_default_config_files(*file_locations)
or using the very first arguments
    --config <CONFIGFILE>
To specify that no config file is to be read:
    --no-config

To check whether a config file is in use:
    ArgumentParserConfig.is_active()

Methods are also provided to read options and their arguments from any
ini-section of the configuration file.
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
class NoValidConfigFile(NoConfigFileSet):
    pass
class NoOptionalsSection(ConfigError):
    pass
class NoUniqueSectionError(ConfigError):
    pass
class WrongValueCountError(ConfigError):
    pass

# Load options from a given config file section.

class _LoadConfigSectionAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        section = values
        if not ArgumentParserConfig.is_active():
            raise NoConfigFileSet(
                f"no config file set while reading from section {section}")
        config_parser = ArgumentParserConfig.get_config_parser()
        if section not in config_parser.sections():
            pr.warning(f"no config file section {section}")
            return
        parser.config_file_section_exec(namespace, values)

_load_config_section_option_parser = argparse.ArgumentParser(add_help=False)

_load_config_section_option_parser.add_argument(
    "--config-section", metavar="SECTION",
    action=_LoadConfigSectionAction,
    help="apply options from SECTION in the config file")

class CLIConfig(Enum):
    """
    Keep track of which option was chosen.
    """
    NO_CHOICE = 0
    SET_NO_CONFIG_FILE = 1
    SET_CONFIG_FILE_UNREAD = 2
    SET_CONFIG_FILE_READ = 3

class ArgumentParserConfigSubparser(argparse.ArgumentParser):
    """
    Drop-in replacement for argparse.ArgumentParser when used as a subparser.
    The top-level parser replacement is given below, as a derived class.

    At parse_known_args time, first run the action for each argument given
    in the config file.
    """
    @classmethod
    def get_from_optionals_section(cls, key, type=None, nargs=None):
        vals = cls.get_from_section(
            key,
            section=ArgumentParserConfig.get_optionals_section(),
            type=type,
            nargs=nargs)
        ArgumentParserConfig.register_read_request(key)
        return vals

    @classmethod
    def get_from_section(cls, key, section, type=None,
                         merge_sections=True, nargs=None):
        """
        Return a list of values corresponding to key from section sect,
        where sect is either the literal section name or a callable predicate.

        If multiple sections match and hold values for the key, either raise
        an exception or merge the results, depending on merge_sections.

        If merge_sections, search fetch from all matching sections. If this is
        false and multiple sections match, raise NoUniqueSectionError.
        """
        if not ArgumentParserConfig.is_active():
            raise NoConfigFileSet
        values_lists = cls._get_values_from_section(key, section, type)
        if not values_lists:
            raise NoSectionError(f"no section found while fetching key {key}")
        values = cls._process_values_nargs(
            values_lists, key, nargs, merge_sections
            )
        return values

    @classmethod
    def _get_values_from_section(cls, key, sect, val_type=None):
        """
        Fetch key values strings from each matching section, apply type to each,
        and return a list of those value lists.
        """
        def process_raw_valuestr(valuestr):
            """
            Return a list of typed values corresponding to the raw config value
            string. Split the config value by line breaks and, if a type was
            provided, apply it to each line.
            """
            substrings = valuestr.split("\n")
            split_raw = filter(lambda v: len(v) > 0, substrings)
            if val_type is not None:
                def guarded_type(val):
                    try:
                        return val_type(val)
                    except Exception as exc:
                        cls_exc = type(exc)
                        msg = \
                            f"while interpreting config value {val}: {str(exc)}"
                        raise cls_exc(msg) from exc
                split_raw = map(guarded_type, split_raw)
            res = list(split_raw)
            return res
        mgr = ArgumentParserConfig.get_config_parser()
        if not callable(sect):
            # We have a string, which should match a single section.
            assert isinstance(sect, str), \
                "_get_values_from_section: expected str, got {sect}"
            values = process_raw_valuestr(mgr.get(sect, key))
            values = [values]
        else:
            matching_sections = [s for s in mgr.sections() if sect(s)]
            if not matching_sections:
                values = []
            else:
                values = []
                for section in matching_sections:
                    try:
                        values.append( \
                            process_raw_valuestr(mgr.get(section, key)))
                    except NoOptionError:
                        pass
        return values

    @classmethod
    def _process_values_nargs(cls, values_lists, key, nargs, merge_sections):
        """
        Given a list of lists of values, pick the correct value or values
        depending on nargs and merge_sections.
        """
        def check_value_count(values_lists, legal_count):
            for values in values_lists:
                if len(values) != legal_count:
                    msg = "%s: expected %s argument(s), got %s" % \
                             (key, legal_count, values)
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
            check_value_count(values_lists, legal_count=1)
            values = to_values(values_lists)
            values = values[-1]
        elif nargs == 0:
            check_value_count(values_lists, legal_count=0)
            values = []
        elif isinstance(nargs, int):
            check_value_count(values_lists, legal_count=nargs)
            values = values_lists[-1]
        elif nargs in ("+", "*"):
            values = to_values(values_lists)
        else:
            assert False, \
                "unexpected nargs: %s" % (nargs,)
        return values

    def config_file_section_exec(self, namespace, section=None):
        """
        For each known parser action, check if any of their option_string is
        present in a certain config file section and, if so, execute it.

        If section is ommited, read from the main optionals section.

        Excluded actions: ChooseConfigFileSet, ChooseConfigFileUnset, help
        actions.
        """
        for action in self._actions:
            if isinstance(action, (ChooseConfigFileSet, ChooseConfigFileUnset)):
                continue
            if hasattr(action, "option_strings") \
                    and action.option_strings \
                    and action.option_strings[0] == "-h": # Skip Help Actions.
                continue
            if hasattr(action, "default") \
                and not hasattr(namespace, action.dest):
                setattr(namespace, action.dest, getattr(action, "default"))
            self._config_file_exec_one_action(action, namespace, section)

    def _config_file_exec_one_action(self, action, namespace, section=None):
        """
        Call this action with the values from the default section of the
        config file (minding nargs), as if the config entry had been input from
        the command line.
        Raise various ConfigError if there are too many or not enough arguments.
        """
        option_strings = action.option_strings
        if not option_strings \
                or option_strings[0][0] != "-" \
                or option_strings[0] == "-h":
            return

        if section is None:
            section = ArgumentParserConfig.get_optionals_section()

        for option_string in option_strings:
            assert option_string[0] == "-", \
                f"_config_file_exec_one_action: invalid: {option_string}"
            cut_option_string = option_string.lstrip("-").replace("-", "_")
            try:
                values = ArgumentParserConfig.get_from_section(
                    key=cut_option_string,
                    section=section,
                    type=action.type,
                    nargs=action.nargs)
            except NoOptionError:
                continue
            except ConfigError as exc:
                pr.warning(str(exc))
                continue
            try:
                action(self, namespace, values, option_string)
            except Exception as exc:
                cls_exc = type(exc)
                msg = "while reading config option " \
                      f"{cut_option_string}: {str(exc)}"
                raise cls_exc(msg) from exc
            break

    def parse_known_args(self, args=None, namespace=None):
        """
        Go through the parser actions and run each whose option string matches
        an entry in the config file default section.
        NB: This is called by ArgumentParser.parse_args().
        """
        if namespace is None:
            namespace = argparse.Namespace()
        if ArgumentParserConfig.is_active():
            self.config_file_section_exec(namespace)
        return super().parse_known_args(args, namespace)

class ArgumentParserConfig(ArgumentParserConfigSubparser):
    """
    Replacement top-level parser.
    Handle config file selection and creating appropriate type subparsers.
    """
    _cfg_mgr = None
    _default_config_files = []
    _optionals_section = "OPTIONALS"
    _all_optional_section_keys = set()

# Class methods:
# Setting the config file locations, fetching.

    @classmethod
    def is_active(cls):
        return cls._cfg_mgr is not None

    @classmethod
    def  set_default_config_files(cls, *filenames):
        ArgumentParserConfig._default_config_files = filenames

    @classmethod
    def  set_optionals_section(cls, section_name):
        ArgumentParserConfig._optionals_section = section_name

    @classmethod
    def  get_optionals_section(cls):
        return ArgumentParserConfig._optionals_section

    @classmethod
    def  get_config_parser(cls):
        assert cls.is_active()
        return ArgumentParserConfig._cfg_mgr

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
        read_files = None
        for fname in filenames:
            if os.path.exists(fname):
                try:
                    read_files = cfg_parser.read(fname)
                except configparser.ParsingError as exc:
                    raise ConfigError(str(exc)) from exc
                if read_files:
                    break
        if not read_files:
            raise NoValidConfigFile(
                "trying to read: %s" % (",".join(filenames),))
        opt_section = ArgumentParserConfig._optionals_section
        if opt_section not in ArgumentParserConfig._cfg_mgr.keys():
            raise NoOptionalsSection(
                f"no section {opt_section} in: {read_files[0]}")
        ArgumentParserConfig._all_optional_section_keys = \
            set(cfg_parser[opt_section].keys())

    @classmethod
    def register_read_request(cls, key):
        ArgumentParserConfig._read_requests.add(key)

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

    def add_subparsers(self, *args, parser_class=None, **kwargs):
        if parser_class is not None:
            assert issubclass(parser_class, ArgumentParserConfigSubparser), \
                "argparse_config: bad custom parser_class"
        else:
            parser_class = ArgumentParserConfigSubparser

        class _HandlerWrapper(object):
            def __init__(self, original_handler):
                self._ap_handler = original_handler

            def add_parser(self, parser_name, *args, **kwargs):
                parents = kwargs.pop("parents", [])
                parents = parents + [_load_config_section_option_parser]
                return self._ap_handler.add_parser(
                    parser_name, *args, parents=parents, **kwargs)

            def __getattr__(self, attr):
                return getattr(self._ap_handler, attr)

        original_handler = super().add_subparsers(
            *args, parser_class=parser_class, **kwargs)
        return _HandlerWrapper(original_handler)

    def parse_known_args(self, args=None, namespace=None):
        """
        Go through the parser actions and run any for which the option string
        matches an entry in the config file default section.
        """
        if args is None:
            args = sys.argv[1:]
        ArgumentParserConfig._read_requests = set()
        def first_switch_matches(min_switch, full_switch):
            if not args:
                return False
            cli_arg = args[0]
            return cli_arg.startswith(min_switch) \
                   and full_switch.startswith(cli_arg)
        if first_switch_matches("--no-conf", "--no-config"):
            fo_val = CLIConfig.SET_NO_CONFIG_FILE
        elif first_switch_matches("--conf", "--config"):
            if len(args) < 3:
                raise argparse.ArgumentError("Missing config file")
            fo_val = CLIConfig.SET_CONFIG_FILE_UNREAD
            self._read_config_files(file_expanduser(args[1]))
        else:
            fo_val = CLIConfig.NO_CHOICE
            try:
                self._read_config_files(
                    *ArgumentParserConfig._default_config_files)
            except NoValidConfigFile:
                pass
        ChooseConfigAction.set_cli_option(fo_val)
        res = super().parse_known_args(args, namespace)
        all_keys = ArgumentParserConfig._all_optional_section_keys
        read_keys = ArgumentParserConfig._read_requests
        if not all_keys.issubset(read_keys):
            unread_keys = all_keys.difference(read_keys)
            pr.info("ignored config file entries: " + str(unread_keys))
        return res

# These options should used before any other action,
# to select the config files.
# At parse time, they do nothing.

class ChooseConfigAction(argparse.Action):
    _config_file_option = None
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
    @classmethod
    def set_cli_option(cls, cli_option):
        cls._config_file_option = cli_option
    @classmethod
    def get_cli_option(cls):
        if cls._config_file_option is None:
            raise RuntimeError("ChooseConfigAction: no cli option set")
        return cls._config_file_option

class ChooseConfigFileSet(ChooseConfigAction):
    """
    Handle --config <FILE>
    """
    def __call__(self, parser, namespace, value, option_string=None):
        super().__call__(parser, namespace, value, option_string)
        if self.get_cli_option() == CLIConfig.SET_NO_CONFIG_FILE:
            self._inconsistent_usage()
        elif self.get_cli_option() == CLIConfig.SET_CONFIG_FILE_READ:
            self._config_options_first()
        assert self.get_cli_option() == CLIConfig.SET_CONFIG_FILE_UNREAD
        try:
            ArgumentParserConfig.set_default_config_files(value)
        except ConfigError as exc:
            pr.error("config file: %s" % (exc))
            sys.exit(1)
        ChooseConfigAction.set_cli_option(CLIConfig.SET_CONFIG_FILE_READ)

class ChooseConfigFileUnset(ChooseConfigAction):
    """
    Handle --no-config
    """
    def __call__(self, parser, namespace, value, option_string=None):
        super().__call__(parser, namespace, value, option_string)
        if self.get_cli_option() in \
                (CLIConfig.SET_CONFIG_FILE_UNREAD,
                 CLIConfig.SET_CONFIG_FILE_READ):
            self._inconsistent_usage()
