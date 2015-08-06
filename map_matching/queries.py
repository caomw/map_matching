from .utils import Edge, Measurement


_EDGE_TABLE_NAME = 'berlin_ways'


_QUERY_EDGES_CLOSE_TO_SEQP = '''
WITH
-- Dump all sequence points
seqp AS (
    SELECT id,
           -- TODO: make it only one dump operation here
           (ST_DumpPoints(path_gm)).geom::geography AS gg,
           (ST_DumpPoints(path_gm)).path[1] AS index
    FROM sequences
    WHERE id=%(seq)s)

SELECT seqp.index, ST_Y(seqp.gg::geometry) AS lat, ST_X(seqp.gg::geometry) AS lon,
       edges.gid, edges.source, edges.target,
       edges.length * 1000, edges.reverse_cost * 1000,
       ST_LineLocatePoint(edges.the_geom, seqp.gg::geometry) AS location,
       ST_Distance(seqp.gg, edges.the_geom::geography) AS distance
FROM seqp CROSS JOIN {edge_table_name} AS edges

-- Filter edges only intersected with bbox of each point
-- Note: ST_Envelope(ST_Buffer(...)) is same as ST_Expand,
--       except that ST_Expand doesn't support geography type yet
WHERE edges.the_geom && ST_Envelope(ST_Buffer(seqp.gg, %(radius)s)::geometry)
      AND ST_DWithin(seqp.gg, edges.the_geom::geography, %(radius)s)
'''.format(edge_table_name=_EDGE_TABLE_NAME)


def query_edges_close_to_seqp(cur, seq_id, radius):
    """
    Query edges within the radius of each sequence point
    (measurement).

    It returns a list of close edges. Each close edge is a tuple of a
    measurement, a edge, closest location from the edge to the
    measurement, and the their distance.
    """
    cur.execute(_QUERY_EDGES_CLOSE_TO_SEQP, {'seq': seq_id, 'radius': radius})
    close_edges = []
    for mid, lat, lon, \
        eid, source, target, cost, reverse_cost, \
        location, distance in cur.fetchall():
        # Measurement
        measurement = Measurement(id=mid, lat=lat, lon=lon)
        # Edge
        edge = Edge(id=eid,
                    start_node=source,
                    end_node=target,
                    cost=cost,
                    reverse_cost=reverse_cost)
        # Closest location from the edge to the sequence point, and
        # the its distance
        assert 0 <= location <= 1
        close_edges.append((measurement, edge, location, distance))
    return close_edges


_QUERY_OUTGOING_EDGES = '''
SELECT gid, source, target, length * 1000, reverse_cost * 1000
FROM {edge_table_name}
WHERE source=%(node)s
'''.format(edge_table_name=_EDGE_TABLE_NAME)


def query_outgoing_edges(cur, node):
    """Query and return a list of edges that start with the input node."""
    cur.execute(_QUERY_OUTGOING_EDGES, {'node': node})
    edges = []
    for eid, source, target, cost, reverse_cost in cur.fetchall():
        assert node == source
        edge = Edge(id=eid,
                    start_node=source,
                    end_node=target,
                    cost=cost,
                    reverse_cost=reverse_cost)
        edges.append(edge)
    return edges


_QUERY_INCOMING_EDGES = '''
SELECT gid, source, target, length * 1000, reverse_cost * 1000
FROM {edge_table_name}
WHERE target=%(node)s
'''.format(edge_table_name=_EDGE_TABLE_NAME)


def query_incoming_edges(cur, node):
    """Query and return a list of edges that end with the input node."""
    cur.execute(_QUERY_INCOMING_EDGES, {'node': node})
    edges = []
    for eid, source, target, cost, reverse_cost in cur.fetchall():
        assert node == target
        edge = Edge(id=eid,
                    start_node=source,
                    end_node=target,
                    cost=cost,
                    reverse_cost=reverse_cost)
        edges.append(edge)
    return edges


def query_undirected_edges(cur, node):
    """
    Query both incoming and outgoing edges of the input node. Reverse
    the incoming edges to outgoing edges like they are undirected
    edges.

    Return a list of edges starting with the input node.
    """
    # TODO: Two queries not so good
    outgoing_edges = query_outgoing_edges(cur, node)
    incoming_edges = query_incoming_edges(cur, node)
    edges = outgoing_edges + map(Edge.reversed_edge, incoming_edges)
    for edge in edges:
        assert node == edge.start_node, \
            'edge must start with node {0}'.format(node)
    return edges


_QUERY_ALL_EDGES = '''
SELECT gid, source, target, length * 1000, reverse_cost * 1000
FROM {edge_table_name}
'''.format(edge_table_name=_EDGE_TABLE_NAME)


def query_all_edges(cur):
    """Query all edges."""
    cur.execute(_QUERY_ALL_EDGES)
    edges = []
    for eid, source, target, cost, reverse_cost in cur.fetchall():
        edge = Edge(id=eid,
                    start_node=source,
                    end_node=target,
                    cost=cost,
                    reverse_cost=reverse_cost)
        edges.append(edge)
    return edges
