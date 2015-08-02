from __future__ import division

import itertools

from geopy import distance

import shortest_path
import viterbi_path
import road_routing

try:
    from itertools import (
        izip as zip,
        imap as map,
        ifilter as filter)
except ImportError:
    pass


# Default parameters copied from OSRM
# See: https://github.com/Project-OSRM/osrm-backend/blob/master/routing_algorithms/map_matching.hpp
# TODO: Need to optimize these parameters
DEFAULT_BETA = 5.0
DEFAULT_SIGMA_Z = 4.07
DEFAULT_MAX_ROUTE_DISTANCE = 2000


class Candidate(object):
    """Candidate object associated to measurements."""
    def __init__(self, measurement, edge, distance, location):
        """
        measurement: an observed location.

        edge: Usually it's the edge close to the measurement. The
        measurement will snap to this edge if it's the winner among a
        set of candidates.

        distance: The distance from the measurement to the edge.

        location: A float between 0 and 1 representing the location of
        the closest point on the edge to the measurement.
        """
        self.measurement = measurement
        self.edge = edge
        self.distance = distance
        self.location = location
        # A dictionary used to store the path from previous candidates
        # to this candidate
        self.path = {}
        # Direction of this candidate. None means unknown; True means
        # it follows the edge and False means it follows the edge in
        # reverse
        self.direction = None

    @property
    def group_key(self):
        """
        The key used to group candidates into states for use in the
        viterbi search.
        """
        return self.measurement.id

    def direction_from(self, source):
        """Guess direction from the previous candidate."""
        # path is a list of edges from TARGET candidate to SOURCE
        # candidate
        path = self.path.get(source)
        if not path:
            return None
        end_edge = path[0]
        return not end_edge.reversed

    def direction_to(self, target):
        """Guess direction to the next candidate."""
        # path is a list of edges from TARGET candidate to SOURCE
        # candidate
        path = target.path.get(self)
        if not path:
            return None
        start_edge = path[-1]
        return not start_edge.reversed


# http://stackoverflow.com/a/5434936
def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def set_directions(candidates):
    for first, second in pairwise(candidates):
        second.direction = second.direction_from(first)
        if first.direction is None:
            first.direction = first.direction_to(second)
    # Note: it's still possible existing some unknown directions
    # (direction == None) after the iteration. Also if there is
    # only single candidate, it's direction is unknown.


class MapMatching(viterbi_path.ViterbiSearch):
    def __init__(self, get_road_edges,
                 max_route_distance=DEFAULT_MAX_ROUTE_DISTANCE,
                 beta=DEFAULT_BETA,
                 sigma_z=DEFAULT_SIGMA_Z):
        self.get_road_edges = get_road_edges
        self.max_route_distance = max_route_distance
        if beta < 0:
            raise ValueError('expect beta to be positive (beta={0})'.format(beta))
        self.beta = beta
        if sigma_z < 0:
            raise ValueError('expect sigma_z to be positive (sigma_z={0})'.format(sigma_z))
        self.sigma_z = sigma_z
        super(MapMatching, self).__init__()

    def calculate_transition_cost(self, source, target):
        max_route_distance = self.calculate_max_route_distance(
            source.measurement, target.measurement)
        try:
            _, route_distance = road_routing.road_network_route(
                (source.edge, source.location),
                (target.edge, target.location),
                self.get_road_edges,
                max_path_cost=max_route_distance)
        except shortest_path.PathNotFound as err:
            # Not reachable
            return -1
        # Geodesic distance based on WGS 84 spheroid
        great_circle_distance = distance.vincenty(
            (source.measurement.lat, source.measurement.lon),
            (target.measurement.lat, target.measurement.lon)).meters
        delta = abs(route_distance - great_circle_distance)
        return delta / self.beta

    def calculate_transition_costs(self, source, targets):
        if not targets:
            return []

        # All measurements in targets should be the same, since they
        # are grouped by measurement ID
        target_measurement = targets[0].measurement

        max_route_distance = self.calculate_max_route_distance(
            source.measurement, target_measurement)
        route_results = road_routing.road_network_route_many(
            (source.edge, source.location),
            [(tc.edge, tc.location) for tc in targets],
            self.get_road_edges,
            max_path_cost=max_route_distance)

        # Geodesic distance based on WGS 84 spheroid
        great_circle_distance = distance.vincenty(
            (source.measurement.lat, source.measurement.lon),
            (target_measurement.lat, target_measurement.lon)).meters

        costs = []
        for target, (path, route_distance) in zip(targets, route_results):
            if route_distance < 0:
                # Not reachable
                costs.append(-1)
                continue
            target.path[source] = path
            delta = abs(route_distance - great_circle_distance)
            costs.append(delta / self.beta)

        # They should be equivalent (only for testing):
        # for cost, target in zip(costs, targets):
        #     single_cost = self.calculate_transition_cost(source, target)
        #     assert abs(cost - single_cost) < 0.0000001

        return costs

    def calculate_emission_cost(self, candidate):
        distance = candidate.distance
        return (distance * distance) / (self.sigma_z * self.sigma_z * 2)

    def calculate_max_route_distance(self, source_mmt, target_mmt):
        return self.max_route_distance

    def offline_match(self, candidates):
        winners = list(self.offline_search(candidates))
        set_directions(winners)
        for winner in winners:
            yield winner

    def online_match(self, candidates):
        last_winner = None
        for winner in self.online_search(candidates):
            # Never know the first winner's direction
            if last_winner is not None:
                winner.direction_from(last_winner)
            yield winner
            last_winner = winner
