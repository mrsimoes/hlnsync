#!/usr/bin/env python

# Copyright (C) 2020 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""
A framework for building parallel class inheritance structures.

Goal
====
Suppose the behaviour of a class and its derived classes is to systematically
depend on an "mode" parameter, constant throughout the existence of each class
instance.

One approach is to have each method contain code to handle the various modes.

Alternatively, for a single class situation one could define a class A with the
common behaviour and subclasses Amode1, Amode2, etc for the each mode.

If B is derived from A and again its behaviour should declense on the mode
parameter, we want to define B, Bmode1, Bmode2, etc with the inheritance
relations Bmode1 -> B -> Amode1 -> A and likewise for each mode.

This is what this framework provides: declare A->B and defined the mode-specific
subclasses and have the inheritance structures for each mode automatically
built.

When creating an instance, x=A(mode=mode1) sets the mode value, and the correct
subclass Amode1 is selected.

Usage
=====
A metaclass modaltype is provided. Then define
    class A(metaclass=modaltype):
        <body of A>
    # and
    class A_modal(A, mode=modename): # The name is unimportant.
        <body of A_modename>

The common class A is called a "mode None" class, while the second is a
declensed class. These are all modal classes.

To create a declensed class, the mode None class must be explicitly created.

To create a modal class derived from another modal class:
    class B(A):
        <body of B>
And the declensed classes:
    class Bmode1(B, mode=mode1name):
        <body of B_mode1>
    class Bmode2(B, mode=mode2name):
        <body of B_mode2>

To create a modal class derived from a non-modal class:
    class B(A, metaclass=modaltype):
        <body of B>
And the declensions are created as before.

Noe each modal class, a mode attribute is set to the correct value, so
that it is available to instances even before __init__.

Implementation
==============
A metaclass modaltype manages the modal class menagerie and creates the modal
classes from the mode None class.

modaltype.__new__ is invoked when the class statement is evaluated:
    class mymodalcls(metaclass=modaltype):
        <body>
    class mymodalcls(mode=mode1):
        <body>

modaltype.__call__ is invoked when modal class instances are created e.g.
    x = mymodalcls(args, ..., mode=mode1)

The following informatin is kept:

If a mode None class A is declared as derived from B and Amode1 is a declension
of A, then
- If B is a modal class, then
    - A.__bases__ == (modalobject,)
    - A._modal_super == B
    - Amode1.__bases__ == (A, Bmode1) # A dummy Bmode1 is created, if needed.
    - Amode1._modal_super == B
- If B is a non-modal class, then
    - A.__bases__ == (modalobject, B)
    - A._modal_super == None
    - Amode1.__bases__ == (A,)
    - Amode1._modal_super == None

Each mode None class has an attribute
    - _modal_declensions, a dict from mode string to the corresponding
    declensed modal class.


Limitations
===========
Each modal class is allowed only one base class.
- No multiple inheritance for modal classes. (It would only require a
reimplementation of the C3 MRO.)
- The same class name cannot be reused across different scopes, as they are
registered globally at modaltype.
"""


class modaltype(type):
    """
    Metaclass for modal classes.
    Manage parallel inheritances, declensed according to a mode parameter.
    """

    @staticmethod
    def _get_declension(none_class, mode):
        """
        Get the mode declension of non_class, creating it if needed.
        """
        if mode in none_class._modal_declensions:
            modal_cls = none_class._modal_declensions[mode]
        else:
            new_cls_name = none_class.__name__ + "_" + mode
            # Call the correct metaclass to create a dummy modal class.
            modal_cls = \
                none_class.__class__(new_cls_name, (none_class,), {}, mode=mode)
        return modal_cls

    def __call__(given_cls, *args, mode=False, **kwargs):
        """
        Create an instance of given_cls with given mode.

        Called when creating an instance of a class of type modaltype.
        If given_cls is a mode None class, select the required declension class,
        if one exists, or create one if needed.
        """
        if given_cls.mode is None: # We are given a mode None class
            if mode in (False, None):
                modal_cls = given_cls
            else:
                assert isinstance(mode, str), "invalid mode: %s" % (mode,)
                modal_cls = modaltype._get_declension(given_cls, mode)
        else:
            assert mode is False or given_cls.mode == mode, "mismatched modes"
            modal_cls = given_cls
        # At this point, modal_cls has the mode attribute correctly set.
        res = type.__call__(modal_cls, *args, **kwargs) # Create and init.
        return res

    def __new__(mcs, name, bases, attrs, *_args, mode=None, **_kwargs):
        """
        Create a new modal type.
         -bases is either empty or a single class.
        If mode is None, create a mode None class derived from the given base,
        which must be either mode none or non-modal.
        Otherwise, create a declension of the given mode none base.
        """
        if len(bases) > 1:
            raise TypeError("modal: multiple inheritance not supported")
        if mode is None:
            # Create a new mode None class.
            if bases and isinstance(bases[0], modaltype): # From a modal class.
                modal_super = bases[0]
                assert modal_super.mode is None
                modal_bases = (modal_super,)
            else:                           # Derived from a non-modal class.
                modal_super = None
                if bases:
                    non_modal_super = bases[0]
                    modal_bases = (non_modal_super,)
                else:
                    modal_bases = ()
            none_cls = super().__new__(mcs, name, modal_bases, attrs)
            none_cls.mode = None
            none_cls._modal_super = modal_super
            none_cls._modal_declensions = {}
            result = none_cls
        else:
            # Create a declensed modal class from none_cls + mode.
            assert len(bases) == 1, "exactly one base needed to declense"
            base_declared = bases[0]
            assert isinstance(base_declared, modaltype)\
                        and base_declared.mode is None
            assert not mode in base_declared._modal_declensions, \
                        "declension already defined"
            modal_super = base_declared._modal_super
            if modal_super is None:
                modal_bases = (base_declared,)
            else:
                modal_super_same_mode = \
                    modaltype._get_declension(modal_super, mode)
                modal_bases = (base_declared, modal_super_same_mode)
            modal_cls = super().__new__(mcs, name, modal_bases, attrs)
            modal_cls._modal_super = modal_super
            modal_cls.mode = mode
            base_declared._modal_declensions[mode] = modal_cls
            result = modal_cls
        return result

ONLINE = "online"
OFFLINE = "offline"
MODES = (None, ONLINE, OFFLINE)

class onofftype(modaltype):
    known_modes = MODES
    def __new__(mcls, *args, mode=None, **kwargs):
        assert mode in MODES
        return super().__new__(mcls, *args, mode=mode, **kwargs)
