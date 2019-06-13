#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
A framework for maintaining alternative class inheritance structures,
to be chosen on a per-class instance basis.

Services provided:

- Define a class MyClass with subclasses MyClassOn and MyClassOff. When creating
a new instance x=MyClass(mode=str), the type of x will be MyClassOn or
MyCallOff, depending on whether str is "on" or "off". If unspecified, default
to "on". If MyClassOff wasn't defined, then the type of MyClass("off") is just
MyClass and likewise for MyClassOn.

- This package allows defining MySubClass again with subclasses MySubClassOn
and MySubClassOff so that x=MySubClass(mode="off") creates an instance of a type
whose MRO is MySubClassOff->MySubClass-> MyClassOff->MyClass.

- Instances created through OnOff have each a mode attribute set to the correct
value.

How to use:

Define MyClass to have OnOffObject as base class and set the class attribute
_onoff_super to OnOffObject.

Define MySubClass to have OnOffObject as base class and set _onoff_super to
MyClass.

Classes managed by OnOff must have as only stated base the OnOffObject class and
must have class attribute _onoff_super. _onoff_super may be OnOff-manager or
not.

Implementation:
Special _onoff attributes no each managed class:
    _onoff_super = must be set when class is defined, either a single
        OnOff-managed class or a tuple of classes, the first of which is OnOff-
        managed.
    _onoff_mro = the mro for this OnOff class, in which -On -Off
        children are omitted (the mro is the same for both modes)
    _onoff_typecache_on and _onoff_typecache_off = the immediate subclass
        for each case, whether user-provided or created by this module.

TODO: This is incomplete: it does not support multiple inheritance in the
parallel structure via setting _onoff_super to a tuple of classes. It needs only
to reimplement C3 here.
The implementation is rudimentary and ugly. On the plus side, it works.
"""

_MODE_TO_SUFFIX = {"online":"Online", "offline":"Offline"}
_MODES = _MODE_TO_SUFFIX.keys()
_SUFFIXES = _MODE_TO_SUFFIX.values()

def _is_managed_main(cls):
    if not hasattr(cls, "_onoff_super"):
        return False
    assert cls.__base__ == OnOffObject
    direct_subs = {n.__name__ for n in cls.__subclasses__()}
    assert direct_subs.issubset({cls.__name__+suf for suf in _SUFFIXES}), \
            "Illegal subclass in %s" % (cls,)
    return True

def _is_managed_option(cls):
    if not hasattr(cls.__base__, "_onoff_super"):
        return False
    assert any(cls.__name__.endswith(s) for s in _SUFFIXES), \
        "Badly named OnOff option: %s" % (cls,)
    return True

def _pick_child(cls, mode):
    """Return the correct subclass of managed cls, creating one if needed."""
    assert mode in _MODES
    for k in cls.__subclasses__():
        if k.__name__.endswith(_MODE_TO_SUFFIX[mode]):
            return k
    return type(cls.__name__ + _MODE_TO_SUFFIX[mode], (cls,), {})

class OnOffObject(object):
    _uniq = 0
    def __new__(cls, *args, **kwargs):
        """cls must be an OnOff-managed class."""
        assert _is_managed_main(cls)
        mode = kwargs.get("mode", "online")
        assert mode in _MODES
        if not hasattr(cls, "_onoff_mro"):
            onoff_mro = [cls]
            cur_cls = cls
            while cur_cls.__base__ == OnOffObject or cur_cls.__base__ != object:
                if cur_cls.__base__ == OnOffObject:
                    nxt_cls = getattr(cur_cls, "_onoff_super")
                    assert isinstance(nxt_cls, type), \
                        "super of %s not a class: %s" % (cur_cls, nxt_cls)
                    cur_cls = nxt_cls
                    onoff_mro.append(cur_cls)
            onoff_mro.append(cur_cls.__base__)
            setattr(cls, "_onoff_mro", tuple(onoff_mro))
        if hasattr(cls, "_onoff_typecache_" + mode):
            newcls = getattr(cls, "_onoff_typecache_" + mode)
        else:
            # Create a type that implements this mro for this mode.
            mode_mro = (_pick_child(mro_cls, mode)
                        for mro_cls in getattr(cls, "_onoff_mro"))
            newcls_name = \
                cls.__name__ + _MODE_TO_SUFFIX[mode] + str(OnOffObject._uniq)
            OnOffObject._uniq += 1
            newcls = type(newcls_name, tuple(mode_mro), {})
            newcls.mode = mode
            setattr(cls, "_onoff_typecache_" + mode, newcls)
        return super(OnOffObject, cls).__new__(newcls)
