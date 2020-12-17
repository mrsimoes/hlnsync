#!/usr/bin/env python

"""
Extend argparse to read non-positional arguments from a config file.

Copyright (C) 2020 Miguel Simoes, miguelrsimoes[a]yahoo[.]com

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
the config files before creating ArgumentParserConfig Instances.
"""

import argparse
import configparser

ConfigError = configparser.Error
NoSectionError = configparser.NoSectionError
NoOptionError = configparser.NoOptionError
class NoUniqueSectionError(configparser.Error):
    pass

class ArgumentParserConfig(argparse.ArgumentParser):
    """
    Drop-in replacement for argparse.ArgumentParser that reads argument
    values from a config file.
    """

    _cfg_mgr = None

    @classmethod
    def set_cfg_locations(cls, *filenames):
        ArgumentParserConfig._cfg_mgr = configparser.ConfigParser()
        if filenames:
            try:
                ArgumentParserConfig._cfg_mgr.read(filenames)
            except configparser.ParsingError as exc:
                raise ConfigError(str(exc)) from exc

    @classmethod
    def get_from_default(cls, key, **kwargs):
        """Accepts and passes type= kw argument"""
        return cls.get_from_section(key,
                                    sect=configparser.DEFAULTSECT, **kwargs)

    @classmethod
    def get_from_section(cls, key, sect, type=None):
        # sect is either the literal section name or a callable predicate.
        if cls._cfg_mgr is None:
            return []
        mgr = cls._cfg_mgr
        def process_raw(valuestr):
            split_raw = filter(lambda v: len(v) > 0, valuestr.split("\n"))
            if type is not None:
                split_raw = map(type, split_raw)
            return list(split_raw)
        if not callable(sect):
            return process_raw(mgr.get(sect, key))
        else:
            res = [process_raw(mgr.get(s, key)) \
                    for s in mgr.sections() if sect(s)]
            if not res:
                raise NoSectionError("no section found for %s" % (key,))
            elif len(res) > 1:
                raise NoUniqueSectionError("multiple sections for %s" % (key,))
            else:
                return res[0]

    def parse_known_args(self, args=None, namespace=None):
        if namespace is None:
            namespace = argparse.Namespace()
        for action in self._actions:
            if hasattr(action, "option_strings") \
                    and action.option_strings \
                    and action.option_strings[0] == "-h": # Skip Help Actions.
                continue
            if hasattr(action, "default"):
                setattr(namespace, action.dest, getattr(action, "default"))
            self._action_cfg_call(action, namespace)
        return super().parse_known_args(args, namespace)

    def _action_cfg_call(self, action, namespace, option_strings=None):
        if ArgumentParserConfig._cfg_mgr is None:
            return
        if option_strings is None:
            option_strings = action.option_strings
        if not option_strings \
                or option_strings[0][0] != "-" \
                or option_strings[0] == "-h":
            return
        for option_string in option_strings:
            assert option_string[0] == "-"
            option_string = option_string.lstrip("-").replace("-", "_")
            try:
                values = ArgumentParserConfig.get_from_default(
                    option_string, type=action.type)
            except configparser.NoOptionError:
                continue
            apply_repeatedly = 1  # nargs=0 option applied repeatedly
            if action.nargs is None:
                if not values:
                    msg = "%s: expected 1 argument"
                    raise ConfigError(msg % option_string)
                values = values[0]
            elif action.nargs == 0:
                if len(values) == 1:
                    try:
                        apply_repeatedly = int(values[0])
                        values = None
                    except:
                        msg = "%s: expected nothing or repeat count, got %s"
                        raise ConfigError(msg % (option_string, values[0]))
                elif len(values) > 1:
                    msg = "%s: expected no arguments or repeat count"
                    raise ConfigError(msg % (option_string,))
            elif isinstance(action.nargs, int):
                if len(values) != action.nargs:
                    msg = "%s: expected %d args, got %d"
                    raise(ConfigError(
                        msg % (option_string, action.nargs, len(values))))
            elif action.nargs == "+":
                if not values:
                    msg = "%s: expected at least one argument, got %d"
                    raise ConfigError(msg % (option_string, len(values)))
            else:
                assert action.nargs == "*"
            for _ in range(apply_repeatedly):
                action(self, namespace, values, option_string)
            break
