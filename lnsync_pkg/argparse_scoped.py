# Copyright (C) 2021 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
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

Purpose:
========
Extend argparse with scoped arguments: associated positional and optional
arguments set up so that a scoped optional argument can act on a subset of its
associated scoped positionals: either ALL, only the NEXT (whole group or
NEXT_SINGLE), or ALL subsequent in the command line.

Usage:
======
To define a scoped positional argument, set in add_argument the action to a
class derived from ScPosArgAction. This will keep track of all scoped
positional arguments as they are parsed and apply any appropriate scoped
optional argument action.

TODO: use the namespace list to bind optionals and positionals

To define a scoped optional argument, set in the argparse add_argument call
the action to class derived from ScOptArgAction, and set sc_scope to either
ALL, NEXT_ARG, NEXT_SINGLE, or SUBSEQUENT. This action records what needs to be
applied to upcoming scoped positional arguments.

On the scoped optional arparse call, sc_dest and sc_default may also be set.
As in argparse Actions, sc_dest will be available as a property of the
ScOptArgAction instance.

Methods of ScOptArgAction that may be overriden:

As argparse actions can record values in a namespace, so scoped positional
argument properties can record values in private namespaces, one for each
positional argument. The ScOptArgAction.sc_get_namespace retrieves the private
namespace corresponding to a positional argument value (the value returned from
it's type). This must be implemented in the derived class.

- sc_get_namespace(self, parser, namespace, pos_val)
    Called to get the object that will get the positional argument attributes.

To effect the action, provide an sc_action method, with signature
    (parser, namespace, pos_arg, values, option_string)
(This is different from the argparse Action API, usually followed by overriding
__call_. Note also the extra parameter pos_arg. __call__ may also be overriden.)

- sc_action(self, parser, namespace, pos_val, opt_val, option_string=None)
    Called for scoped positional args within scope of the scoped optional.

sc_action may instead be specified with a string when calling add_argument:
- "store": store the value into sc_namespace, under sc_dest.
- "store_bool": store true default, and also interpret options prefixed
   by "--no-") This forces nargs to be 0.
- "append": append to a list of values, starting from an empty list.
These presets also record values to the global namespace when the scope is ALL.

Even if a scoped optional argument isn't present in the command line, a default
action can still be specified:

- sc_apply_default(self, parser, namespace, pos_val)
    Called once for each positional args, regardless of whether the option
    occurs in the command line.
    The default version gets the pos_val namespace and the attribute name
    self.sc_dest and sets it to sc_default.

Implementation:
===============
Each occurrence of an optional scoped argument is recorded as an _OptionCall
instance and added to a list in the parser namespace. These are used to apply
the action to subsequent scoped positional arguments.

All argparse_scoped namespace data is stored in a _argparse_scoped_data attribute.
"""

# pylint: disable=function-redefined, protected-access

import argparse
import abc
from enum import Enum

import lnsync_pkg.printutils as pr
from lnsync_pkg.miscutils import append_to_namespace_list

class Scope(Enum):
    ALL = 1
    NEXT = 2
    NEXT_SINGLE = 3
    SUBSEQUENT = 4

class _OptionCall:
    """
    An occurrence of a scoped optional argument in the command line.
    """
    def __init__(self, parser, namespace, opt_val, option_string, opt_action):
        self.parser = parser
        self.namespace = namespace # Parser namespace.
        self.opt_val = opt_val # Scoped optional argument values.
        self.option_string = option_string
        self.opt_action = opt_action # Scoped optional arg sc_action instance.

    def _sc_action(self, pos_arg):
        if self.opt_action.sc_scope == Scope.NEXT_SINGLE \
                and isinstance(pos_arg, list):
            pos_arg = pos_arg[:1]
        pr.trace("applying: %s %s", self.option_string, self.opt_val)
        self.opt_action.sc_action(self.parser, self.namespace,
                                  pos_arg, self.opt_val, self.option_string)

class _SCPrivateData:
    """
    Track pending otion calls and parsed positional arguments.
    Should be a property of the parser namespace, stored in attribute
    named DATA_ATTRIBUTE, set below.
    """
    _DATA_ATTRIBUTE = "_sc_arparse_scoped_data"

    @staticmethod
    def for_namespace(namespace):
        data_obj = getattr(namespace, _SCPrivateData._DATA_ATTRIBUTE, None)
        if data_obj is None:
            data_obj = _SCPrivateData(namespace)
            setattr(namespace, _SCPrivateData._DATA_ATTRIBUTE, data_obj)
        return data_obj

    def __init__(self, namespace):
        self.namespace = namespace

    def append_to_private_list(self, attribute_name, val):
        append_to_namespace_list(self, attribute_name, [val])

    def get_private_list(self, attribute_name):
        attr_list = getattr(self, attribute_name, [])
        if attr_list is None:
            setattr(self, attribute_name, attr_list)
        return attr_list

    def filter_private_list(self, attribute_name, filter_fn):
        attr_list = getattr(self, attribute_name, [])
        attr_list = [val for val in attr_list if filter_fn(val)]
        setattr(self, attribute_name, attr_list)

    def store_option_call(self, option_call):
        self.append_to_private_list("sc_option_calls", option_call)

    def store_pos_arg(self, pos_arg):
        self.append_to_private_list("sc_pos_args", pos_arg)

    def iter_pos_args(self):
        for val in self.get_private_list("sc_pos_args"):
            yield val

    def iter_option_calls(self):
        for val in self.get_private_list("sc_option_calls"):
            yield val

    def filter_option_calls(self, filter_fn):
        self.filter_private_list("sc_otion_calls", filter_fn)


class ScOptArgAction(argparse.Action):
    """
    Abstract base for scoped optional argument actions.
    """

    def __init__(self, *args, sc_scope=None, sc_action=None,
                 nargs=None, **kwargs):
        dest = kwargs.pop("dest")
        self.sc_dest = kwargs.pop("sc_dest", dest)
        if sc_action == "store_bool":
            nargs = 0
        super().__init__(*args, dest=dest, nargs=nargs, **kwargs)
        if "default" in kwargs:
            self.sc_default = kwargs["default"]
        assert isinstance(sc_scope, Scope), \
            f"ScOptArgAction: not a scope: {sc_scope}"
        self.sc_scope = sc_scope
        if sc_action == "store":
            self.sc_action = self._sc_action_store
        elif sc_action == "store_bool":
            self.sc_action = self._sc_action_store_bool
        elif sc_action == "append":
            self.sc_action = self._sc_action_append
        elif callable(sc_action):
            self._sc_action_callable = sc_action
            self.sc_action = self._sc_action_custom

    def _sc_action_custom(
            self, _parser, namespace, pos_val, _opt_val, option_string):
        self._sc_action_callable(
            self, _parser, namespace, pos_val, _opt_val, option_string)

    def _sc_action_store(self, _parser, namespace,
                         pos_val, opt_val, _option_string):
        sc_ns = self.sc_get_namespace(pos_val)
        setattr(sc_ns, self.sc_dest, opt_val)
        if self.sc_scope == Scope.ALL:
            setattr(namespace, self.dest, opt_val)

    def _sc_action_store_bool(self, _parser, namespace,
                              pos_val, _opt_val, option_string):
        sc_ns = self.sc_get_namespace(pos_val)
        if option_string[0:5] == "--no-":
            val = False
        else:
            val = True
        setattr(sc_ns, self.sc_dest, val)
        if self.sc_scope == Scope.ALL:
            setattr(namespace, self.dest, val)

    def _sc_action_append(self, _parser, namespace,
                          pos_val, opt_val, _option_string):
        def do_update_namespace(namespace, key, new_elem_or_list):
            """
            Append new_elem_or_list to existing list at namespace.key.
            new_elem_or_list is either a list or a single value,
            depending on self.nargs.
            """
            if self.nargs is not None:
                assert isinstance(new_elem_or_list, list), \
                    f"ScOptArgAction: not a list: {new_elem_or_list}"
                to_add = new_elem_or_list
            else:
                to_add = [new_elem_or_list]
            append_to_namespace_list(namespace, key, to_add)
        do_update_namespace(
            self.sc_get_namespace(pos_val),
            self.sc_dest,
            opt_val)
        if self.sc_scope == Scope.ALL:
            do_update_namespace(namespace, self.dest, opt_val)

    @abc.abstractmethod
    def sc_get_namespace(self, pos_val):
        pass

    def sc_apply_default(self, _parser, namespace, pos_val):
        """
        Apply this option in default form to the pos_arg object.
        """
        if hasattr(self, "sc_default"):
            sc_ns = self.sc_get_namespace(pos_val)
            if not hasattr(sc_ns, self.sc_dest):
                setattr(sc_ns, self.sc_dest, self.sc_default)
#   iffy TODO
        if hasattr(self, "default"):
            if not hasattr(namespace, self.dest):
                setattr(namespace, self.dest, self.default)

    def __call__(self, parser, namespace, opt_val, option_string=None):
        this_opt_call = _OptionCall(parser, namespace,
                                    opt_val, option_string, self)
        sc_data = _SCPrivateData.for_namespace(namespace)
        sc_data.store_option_call(this_opt_call)
        if self.sc_scope == Scope.ALL:
            for pos_arg in sc_data.iter_pos_args():
                this_opt_call._sc_action(pos_arg)

class ScPosArgAction(argparse.Action):
    """
    Base for scoped positional argument Actions, for one or zero argument
    values.
    """
    def __call__(self, parser, namespace, pos_arg, option_string=None):
        if self.nargs in ("+", "*"):
            assert isinstance(pos_arg, list), \
                f"ScPosArgAction.__call__ not a list: {pos_arg}"
            for pos_val in pos_arg:
                self._process_pos_arg(parser, namespace, pos_val)
            setattr(namespace, self.dest, pos_arg)
        else:
            assert not isinstance(pos_arg, list), \
                f"ScPosArgAction: should not be a list: {pos_arg}"
            self._process_pos_arg(parser, namespace, pos_arg)
            setattr(namespace, self.dest, pos_arg)

    def _process_pos_arg(self, parser, namespace, pos_arg):
        self._fill_in_defaults(parser, namespace, pos_arg)
        sc_data = _SCPrivateData.for_namespace(namespace)
        # Apply preceding opt args.
        for opt_call in sc_data.iter_option_calls():
            opt_call._sc_action(pos_arg)
        # Discard NEXT and NEXT_SINGLE optionals.
        sc_data.filter_option_calls(
            lambda opt: \
                opt.opt_action.sc_scope \
                    not in (Scope.NEXT, Scope.NEXT_SINGLE))
        sc_data.store_pos_arg(pos_arg)

    @staticmethod
    def _fill_in_defaults(parser, namespace, pos_arg):
        for action in parser._actions:
            if isinstance(action, ScOptArgAction):
                action.sc_apply_default(parser, namespace, pos_arg)
