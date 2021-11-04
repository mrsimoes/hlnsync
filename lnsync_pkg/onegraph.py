#!/usr/bin/python3

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
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
Operations on one-graphs.

A one-graph is a directed graph with at most one arrow out of each node.
Operations include finding root nodes and checking for cycles.
"""

class OneGraph:
    """
    Implement a directed 1-graph with efficient operations on cycles,
    but no direct access to the node set.

    A directed 1-graph has at most one arrow out of each node.
    A node is any hashable value, except None.
    Operations:
    - add_arrow(a, b) creates a new arrow a->b.
    - remove_arrow(a, b) removes the arrow a->b.
    """

    def __init__(self):
        self._arrows = {} # Set of arrows as dictionary {node_from:node_to, ...}.
        self._cycles = [] # List of cycles, each cycle a set of nodes.

    def __str__(self):
        return str(self._arrows)

    def follow_arrow(self, node_a):
        """
        If a->b is in the graph, return b, else return None.
        """
        if node_a in self._arrows:
            return self._arrows[node_a]
        else:
            return None

    def has_cycle(self):
        return self._cycles

    def iter_arrows(self):
        for node_a, node_b in self._arrows.items():
            yield (node_a, node_b)

    def add_arrow(self, node_a, node_b):
        assert not node_a in self._arrows, \
            "OneGraph.add_arrow: arrow already in"
        self._arrows[node_a] = node_b
        cyc = self._cycle_from(node_b)
        if cyc is not None: # There's a cycle. Is it a new one?
            elem = cyc[0] # Pick any elem in the found cycle.
            if not any((elem in old_cyc) for old_cyc in self._cycles):
                self._cycles.append(set(cyc))

    def add_graph(self, other_one_g):
        """
        Add to self all arrows in other_one_g.
        """
        assert self is not other_one_g, \
            "OneGraph.add_graph: adding to itself"
        for node_a, node_b in other_one_g.iter_arrows():
            self.add_arrow(node_a, node_b)

    def remove_arrow(self, node_a, node_b):
        assert node_a in self._arrows \
            and self._arrows[node_a] == node_b, \
                "OneGraph.remove_arrow: arrow not in"
        del self._arrows[node_a]
        for k, cycle in enumerate(self._cycles): # Remove at most one cycle.
            if node_a in cycle:
                del self._cycles[k]
                break

    def remove_graph(self, other_one_g):
        """
        Remove from self all arrows in other_one_g.
        """
        assert self is not other_one_g, \
            "OneGraph.remove_graph: removing from itself"
        for node_a, node_b in other_one_g.iter_arrows():
            self.remove_arrow(node_a, node_b)

    def get_all_roots(self):
        """
        Return the set of all roots (minimal elements).
        Called infrequently in our application - computed on a need-to basis.
        """
        roots = set(self._arrows)
        for _, node_to in self._arrows.items():
            if node_to in self._arrows:
                roots.discard(node_to)
        return roots

    def get_all_leaves(self):
        """
        Return the set of all nodes which are source to no arrow.
        """
        arrow_graph = self._arrows
        leaves = set(arrow_graph.values())
        for node in arrow_graph.values():
            if node in arrow_graph:
                leaves.discard(node)
        return leaves

    def _cycle_from(self, node):
        """
        Return a (list) cycle starting at e, if one exists, None otherwise.
        """
        arrow_gr = self._arrows
        if len(arrow_gr) <= 1:
            return None
        stack = [node]
        while True:
            top = stack[-1]
            if top in arrow_gr:        # There is a next node.
                nxt = arrow_gr[top]
                if nxt in stack:
                    return stack
                else:
                    stack.append(nxt)
            else:
                break
        return None
