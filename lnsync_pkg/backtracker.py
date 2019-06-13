#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""Backtrack search for a maximal state extending the initial state.

A State can be extended by applying 'down' a delta of data describing that
extension. A state may be either valid or not. Invalid states are discarded.
On backtracking, the extension is undone by applying the delta 'up'.
"""

from __future__ import print_function

import abc

import lnsync_pkg.printutils as pr

class SearchState(object):
    """Virtual class for a backtracking solver.

    Subclasses should implement make_delta_iter, down_delta,
    up_delta, and is_valid.
    """

    @abc.abstractmethod
    def make_delta_iter(self):
        """Return either a non-empty delta iterator or None."""

    @abc.abstractmethod
    def down_delta(self, state_delta):
        """Apply a delta to obtain a child state."""

    @abc.abstractmethod
    def up_delta(self, state_delta):
        """Apply a delta to revert from a child state back to the parent."""

    @abc.abstractmethod
    def is_valid(self):
        """Return True is the current state is valid."""

def do_search(state):
    """Backtrack search for a valid leaf state, return True if one was found.
    """
    if not state.is_valid():
        return False
# The search stack contains pairs (last_delta_expanded, deltas_to_explore_iter).
# A newly created deltas_to_explore_iter is either a non-empty iterator or None.
# At an invalid state or when deltas_to_explore is exhausted, backtrack.

    search_stack = [(None, state.make_delta_iter())]

    while True:
        # The state is valid.
        originating_delta, down_deltas_iter = search_stack[-1]
        if down_deltas_iter is None:
            pr.debug("backtracker: success")
            return True # Found a valid leaf.
        try:
            next_delta = down_deltas_iter.next()
        except StopIteration:
            # No more children: backtrack, if possible.
            if originating_delta is None:
                # Exhausted all root children.
                pr.debug("backtracker: failure")
                return False
            else:
                search_stack.pop()
                state.up_delta(originating_delta)
        else: # We have a new delta.
            state.down_delta(next_delta)
            if state.is_valid():
                search_stack.append((next_delta, state.make_delta_iter()))
            else:
                state.up_delta(next_delta)


class QueensBoard(SearchState):
    """Solve the n x n queen problem using depth-first shared state.
    """
    __slots__ = "n", "board", "next_row", "valid"
    def __init__(self, board_size):
        self.board_size = board_size
        self.board = [[0,]*board_size for _ in xrange(board_size)]
        self.next_row = 0
        self.valid = True

    def make_delta_iter(self):
        if self.next_row == self.board_size:
            return None
        else:
            return iter(xrange(self.board_size))

    def down_delta(self, state_delta):
        col = state_delta
        row = self.next_row
        self.board[row][col] = 1
        bd_size = self.board_size
        for delta in xrange(bd_size):
            for row_sign in (-1, 0, 1):
                for col_sign in (-1, 0, 1):
                    (oth_r, oth_c) = (row+row_sign*delta, col+col_sign*delta)
                    if 0 <= oth_r < bd_size and 0 <= oth_c < bd_size \
                            and (oth_r, oth_c) != (row, col):
                        if self.board[oth_r][oth_c] != 0:
                            self.valid = False
                            self.next_row = row + 1
                            return
        self.next_row = row + 1
        self.valid = True


    def up_delta(self, state_delta):
        col = state_delta
        assert self.next_row > 0, "Cannot go up from row 0."
        assert self.board[self.next_row-1][col] != 0, \
            "Queen expected at (%d,%d)" % (self.next_row-1, col)
        self.next_row -= 1
        self.board[self.next_row][col] = 0
        self.valid = True

    def is_valid(self):
        return self.valid

    def __str__(self):
        rep = ""
        for row in self.board:
            for elem in row:
                rep += ("- " if elem == 0 else "* ")
            rep += "\n"
        return rep

def _solve_n_queen():
    import sys
    try:
        assert len(sys.argv) == 2, "Wrong number of arguments."
        board_size = int(sys.argv[1]),
        assert board_size > 0, "Board size must be >= 1."
    except Exception as exc:
        print(exc)
        raise SystemExit(
            "Usage: backtracker <n>\nSolve the n x n queen problem.")
    print("Solving the queens problem with board size %d" % board_size)
    board = QueensBoard(board_size)
    if do_search(board):
        print(board)
    else:
        print("No solution found.")

if __name__ == "__main__":
    _solve_n_queen()
