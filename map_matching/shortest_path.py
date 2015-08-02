import heapq
import collections


Edge = collections.namedtuple('Edge', ['start_node', 'end_node', 'cost'])


class PathNotFound(Exception):
    def __init__(self, source, target, max_path_cost=None):
        message = 'Path from {src} to {tar} not found (max_path_cost={cost})'.format(
            src=source,
            tar=target,
            cost=max_path_cost)
        super(PathNotFound, self).__init__(message)


def _pop_unscanned_edge(pqueue, scanned_nodes):
    """
    Pop out from the pqueue the first edge whose end node is not
    scanned. Return a tuple of Nones if no more unscanned edges found.
    """
    assert pqueue
    while True:
        cost_sofar, edge = heapq.heappop(pqueue)
        if not pqueue or edge.end_node not in scanned_nodes:
            break
    if edge.end_node in scanned_nodes:
        assert not pqueue
        return None, None
    return cost_sofar, edge


def _reconstruct_path(source_node, target_node, scanned_nodes):
    """
    Reconstruct a path from the scanned table.

    It returns an edge sequence along the path and the path cost (sum
    of edges cost).
    """
    path, path_cost = [], 0
    node = target_node
    while node is not None:
        edge = scanned_nodes[node]
        path.append(edge)
        path_cost += edge.cost
        node = edge.start_node

    # Pop out the dummy edge ending with the source node
    if path:
        assert path[-1] == (None, source_node, 0)
        path.pop()

    return path, path_cost


def find_shortest_path(source_node, target_node, get_edges, max_path_cost=None):
    """
    Find shortest path between the source node and the target node in
    the graph.

    `get_edges` is a function which accepts a node and returns the
    edges of the node.

    `max_path_cost` is used to limit the search range. By default
    there is not limit (max_path_cost = Infinity).

    It either returns a tuple (path, path_cost) where a path is a
    sequence of edges (from target to source) and path_cost is the
    path cost, or raises PathNotFound error if path not found.
    """
    scanned_nodes = {}
    pqueue = []
    if max_path_cost is None:
        max_path_cost = float('inf')
    if 0 <= max_path_cost:
        # Start with a dummy edge
        heapq.heappush(pqueue, (0, Edge(None, source_node, 0)))

    while pqueue:
        cost_sofar, edge = _pop_unscanned_edge(pqueue, scanned_nodes)
        if edge is None:
            break
        cur_node = edge.end_node
        scanned_nodes[cur_node] = edge
        if cur_node == target_node:
            return _reconstruct_path(source_node, target_node, scanned_nodes)
        for adj_edge in get_edges(cur_node):
            assert adj_edge.start_node == cur_node
            adj_cost_sofar = cost_sofar + adj_edge.cost
            if adj_cost_sofar <= max_path_cost and adj_edge.end_node not in scanned_nodes:
                heapq.heappush(pqueue, (adj_cost_sofar, adj_edge))

    raise PathNotFound(source_node, target_node, max_path_cost)


def find_many_shortest_paths(source_node, target_nodes, get_edges, max_path_cost=None):
    """
    Like `find_shortest_path`, except that it finds shortest paths
    between the source node to a list of target nodes.

    It returns a list of tuples (path, path_cost). If a path is not
    found, the corresponding tuple will be (None, -1).
    """
    if not target_nodes:
        return []
    scanned_nodes = {}
    pqueue = []
    # None value means the goal is not achieved yet
    goals = {node: None for node in target_nodes}
    unachived_goal_count = len(goals)
    if max_path_cost is None:
        max_path_cost = float('inf')
    if 0 <= max_path_cost:
        # Start with a dummy edge
        heapq.heappush(pqueue, (0, Edge(None, source_node, 0)))

    while pqueue:
        cost_sofar, edge = _pop_unscanned_edge(pqueue, scanned_nodes)
        if edge is None:
            break
        cur_node = edge.end_node
        scanned_nodes[cur_node] = edge
        # If it's a goal, but not achieved yet, store the path
        if cur_node in goals and goals[cur_node] is None:
            goals[cur_node] = _reconstruct_path(source_node, cur_node, scanned_nodes)
            unachived_goal_count -= 1
        if unachived_goal_count == 0:
            break
        for adj_edge in get_edges(cur_node):
            assert adj_edge.start_node == cur_node
            adj_cost_sofar = cost_sofar + adj_edge.cost
            if adj_cost_sofar <= max_path_cost and adj_edge.end_node not in scanned_nodes:
                heapq.heappush(pqueue, (adj_cost_sofar, adj_edge))

    # (None, -1) means path not found, while ([], 0) means an empty
    # path found (it happens when source and target are the same)
    return [goals[node] or (None, -1) for node in target_nodes]


def test_find_shortest_path():
    # The example from http://en.wikipedia.org/wiki/Dijkstra's_algorithm
    adjacency_list = {
        1: (Edge(1, 2, 7), Edge(1, 3, 9), Edge(1, 6, 14),
            Edge(1, 5, 21)),      # An extra edge here which is not the shortest
        2: (Edge(2, 1, 7), Edge(2, 3, 10), Edge(2, 4, 15)),
        3: (Edge(3, 1, 9), Edge(3, 2, 10), Edge(3, 4, 11), Edge(3, 6, 2)),
        4: (Edge(4, 2, 15), Edge(4, 3, 11), Edge(4, 5, 6)),
        5: (Edge(5, 4, 6), Edge(5, 6, 9)),
        6: (Edge(6, 1, 14), Edge(6, 3, 2), Edge(6, 5, 9))}

    def _get_edges(node):
        return adjacency_list.get(node, [])

    # It should find correct path
    _, cost = find_shortest_path(1, 5, _get_edges)
    assert cost == 20
    path, cost = find_shortest_path(1, 1, _get_edges)
    assert path == []
    assert cost == 0
    _, cost = find_shortest_path(1, 5, _get_edges, 20)
    assert cost == 20
    _, cost = find_shortest_path(1, 4, _get_edges, 20)
    assert cost == 20

    # It should not find a path
    from nose.tools import assert_raises
    assert_raises(PathNotFound, find_shortest_path, 1, 4, _get_edges, 19)
    assert_raises(PathNotFound, find_shortest_path, 1, 'no such node', _get_edges)
    assert_raises(PathNotFound, find_shortest_path, 'no such node', 1, _get_edges)

    # It should return multiple paths
    paths = find_many_shortest_paths(1, [3, 6, 4, 5, 2], _get_edges)
    assert paths == \
        [([Edge(start_node=1, end_node=3, cost=9)], 9),
         ([Edge(start_node=3, end_node=6, cost=2),
           Edge(start_node=1, end_node=3, cost=9)], 11),
         ([Edge(start_node=3, end_node=4, cost=11),
           Edge(start_node=1, end_node=3, cost=9)], 20),
         ([Edge(start_node=6, end_node=5, cost=9),
           Edge(start_node=3, end_node=6, cost=2),
           Edge(start_node=1, end_node=3, cost=9)], 20),
         ([Edge(start_node=1, end_node=2, cost=7)], 7)]

    # It should give (None, -1) if path not found
    paths = find_many_shortest_paths(1, [3, 'no such node', 2], _get_edges)
    assert paths == \
        [([Edge(start_node=1, end_node=3, cost=9)], 9),
         (None, -1),
         ([Edge(start_node=1, end_node=2, cost=7)], 7)]

    # It should return empty path if target node set is empty
    paths = find_many_shortest_paths(1, [], _get_edges)
    assert paths == []

    # It should return a list of Nones if it is not reachable to any
    # target node from the source node
    paths = find_many_shortest_paths('sorry no such node', [1, 2, 3, 4, 5], _get_edges)
    assert paths == [(None, -1)] * 5
