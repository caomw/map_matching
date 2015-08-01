import collections
import functools


EdgeTuple = collections.namedtuple(
    'Edge',
    ['id', 'start_node', 'end_node',
     'cost', 'reverse_cost',
     # True if it follows the OSM way in reverse, i.e. True if the OSM
     # way is from node A to node B and the edge is from B to A
     'reversed'])


# A simple wrapper of EdgeTuple to make "reversed" default to False
Edge = functools.partial(EdgeTuple, reversed=False)


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


def reversed_edge(edge):
    """Return a new reversed edge."""
    return Edge(id=edge.id,
                start_node=edge.end_node,
                end_node=edge.start_node,
                cost=edge.reverse_cost,
                reverse_cost=edge.cost,
                reversed=not edge.reversed)


def same_edge(left, right, precision=0):
    """Return if two edges are the same."""
    return left.id == right.id \
        and left.start_node == right.start_node \
        and left.end_node == right.end_node \
        and abs(left.cost - right.cost) <= precision \
        and abs(left.reverse_cost - right.reverse_cost) <= precision \
        and left.reversed == right.reversed


def test_same_edge():
    edge = Edge(id=1, start_node=1, end_node=2, cost=3, reverse_cost=4)
    assert same_edge(edge, edge)
    the_reversed_edge = reversed_edge(edge)
    assert not same_edge(edge, the_reversed_edge)
    assert same_edge(edge, reversed_edge(the_reversed_edge))
