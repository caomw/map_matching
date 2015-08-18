import collections
import functools


EdgeTuple = collections.namedtuple(
    'Edge',
    ['id', 'start_node', 'end_node',
     'cost', 'reverse_cost',
     # True if it follows the OSM way in reverse, i.e. True if the OSM
     # way is from node A to node B and the edge is from B to A
     'reversed'])


# A simple wrapper of EdgeTuple with "reversed" default to False, and
# with some custom methods added
class Edge(EdgeTuple):
    """Edge(id, start_node, end_node, cost, reverse_cost, reversed)"""

    # To save space in objects
    __slots__ = ()

    def __new__(_cls, id, start_node, end_node, cost, reverse_cost, reversed=False):
        """Create new instance of Edge(id, start_node, end_node, cost, reverse_cost, reversed)"""
        return tuple.__new__(_cls, (id, start_node, end_node, cost, reverse_cost, reversed))

    def reversed_edge(self):
        """Create a new edge which is reverse to self."""
        reverse = Edge(id=self.id,
                       start_node=self.end_node,
                       end_node=self.start_node,
                       cost=self.reverse_cost,
                       reverse_cost=self.cost,
                       reversed=not self.reversed)
        return reverse

    def same_edge(self, other, precision=0):
        """
        Test if it is the same edge with the other. While comparing costs,
        if their difference is under the precision, then they are
        considered as the same edge.
        """
        return self.id == other.id \
            and self.start_node == other.start_node \
            and self.end_node == other.end_node \
            and abs(self.cost - other.cost) <= precision \
            and abs(self.reverse_cost - other.reverse_cost) <= precision \
            and self.reversed == other.reversed

    def __eq__(self, other):
        return self.same_edge(other, precision=0)


def test_edge():
    edge = Edge(id=1, start_node=1, end_node=2, cost=3, reverse_cost=4)
    assert edge.reversed is False

    # It should be same edge
    same_edge = Edge(id=1, start_node=1, end_node=2, cost=3, reverse_cost=4)
    assert edge.same_edge(same_edge)
    assert edge == same_edge

    # It should not be same edge with its reverse edge
    reversed_edge = same_edge.reversed_edge()
    assert reversed_edge.reversed is True
    assert not edge.same_edge(reversed_edge)
    assert edge != reversed_edge
    assert edge.same_edge(reversed_edge.reversed_edge())
    assert edge == reversed_edge.reversed_edge()


Measurement = collections.namedtuple('Measurement', ['id', 'lat', 'lon'])


class DynamicDict(dict):
    """
    A dict subclass that can dynamically load and cache the values of
    a key when the key is not found for the first time.

    It's like `collections.defaultdict`, except that the factory
    function accepts key as argument.
    """
    def __init__(self, factory):
        self.factory = factory

    def __missing__(self, key):
        values = self.factory(key)
        self[key] = values
        return values


def test_dynamic_dict():
    double = lambda n: n + n
    ddict = DynamicDict(double)
    # It should not miss existing ones
    ddict.update({1: 'hello', 2: 'world'})
    assert ddict[1] == 'hello' and ddict[2] == 'world'
    # It should load 3
    assert ddict[3] == 6
    # It should cache 3
    assert 3 in ddict
