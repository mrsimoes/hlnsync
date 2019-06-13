#!/usr/bin/env python

# Copyright (C) 2018 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
# For conditions of distribution and use, see copyright notice in lnsync.py

"""Operations on one-graphs.

A one-graph is a directed graph with at most one arrow out of each node.
Operations include finding root nodes and checking for cycles.
"""

class OneGraph(object):
    """A (directed) 1-graph - at most one arrow out of each node.

    A node is any hashable value, except None.
    - add_arrow(a,b) creates a new arrow and either 0, 1, or 2 nodes.
    - remove_arrow(a,b) removes an arrow and either 0, 1, or 2 nodes.
    """
    def __init__(self):
        self._arrows = {}
        self._root_to_cycle = {}
        self._cycles = [] # Each cycle in the list is a set of nodes.
    def __str__(self):
        return str(self._arrows)
    def follow_arrow(self, node_a):
        """If a->b is in the graph, return b, else return None."""
        if node_a in self._arrows:
            return self._arrows[node_a]
        else:
            return None
    def has_cycle(self):
        """Return True if a cycle exists, else False."""
        return len(self._cycles) > 0
    def iterarrows(self):
        for node_a, node_b in self._arrows.iteritems():
            yield (node_a, node_b)
    def add_arrow(self, node_a, node_b):
        assert not node_a in self._arrows, \
            "OneGraph.add_arrow: arrow already in"
        self._arrows[node_a] = node_b
        cyc = self._cycle_from(node_b)
        if cyc is not None: # There is a cycle. Is it a new one?
            elem = cyc[0] # Pick any elem in the cycle found.
            if not any((elem in old_cyc) for old_cyc in self._cycles):
                self._cycles.append(set(cyc))
    def add_graph(self, other_one_g):
        """Add to self all arrows in other_one_g."""
        for node_a, node_b in other_one_g.iterarrows():
            self.add_arrow(node_a, node_b)
    def remove_arrow(self, node_a, node_b):
        assert node_a in self._arrows \
            and self._arrows[node_a] == node_b, \
                "OneGraph.remove_arrow: arrow not in"
        del self._arrows[node_a]
        for k in xrange(len(self._cycles)): # Remove at most one cycle.
            if node_a in self._cycles[k]:
                del self._cycles[k]
                break
    def remove_graph(self, other_one_g):
        """Remove from self all arrows in other_one_g."""
        for node_a, node_b in other_one_g.iterarrows():
            self.remove_arrow(node_a, node_b)
    def get_all_roots(self):
        """Return the set of all roots (minimal elements)."""
        roots = set(self._arrows.keys())
        for _, node_to in self._arrows.iteritems():
            if node_to in self._arrows:
                roots.discard(node_to)
        return roots
    def get_all_leaves(self):
        """Return the set of all nodes which are source to no arrow."""
        arrow_graph = self._arrows
        leaves = set(arrow_graph.values())
        for node in arrow_graph.values():
            if node in arrow_graph:
                leaves.discard(node)
        return leaves
    def _cycle_from(self, node):
        """Return a (list) cycle starting at e, if one exists, None otherwise.
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
