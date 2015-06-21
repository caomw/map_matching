from __future__ import division

from geopy import distance

import shortest_path
import viterbi_path
import road_routing


# Default parameters copied from OSRM
# See: https://github.com/Project-OSRM/osrm-backend/blob/master/routing_algorithms/map_matching.hpp
# TODO: Need to optimize these parameters
DEFAULT_BETA = 5.0
DEFAULT_SIGMA_Z = 4.07
DEFAULT_MAX_ROUTE_DISTANCE = 2000


def _timestamp_key(candidate_body):
    """
    Return measure ID which will be used to sort and group candidates.
    """
    measure, _, _, _ = candidate_body
    return measure.id


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

    def calculate_transition_cost(self, source_candidate, target_candidate):
        source_mmt, source_edge, source_location, _ = source_candidate.body
        target_mmt, target_edge, target_location, _ = target_candidate.body
        try:
            _, route_distance = road_routing.road_network_route(
                (source_edge, source_location),
                (target_edge, target_location),
                self.get_road_edges,
                max_path_cost=self.max_route_distance)
        except shortest_path.PathNotFound as err:
            # Not reachable
            return -1
        # Geodesic distance based on WGS 84 spheroid
        bird_fly_distance = distance.vincenty(
            (source_mmt.lat, source_mmt.lon),
            (target_mmt.lat, target_mmt.lon)).meters
        delta = abs(route_distance - bird_fly_distance)
        return delta / self.beta

    def calculate_transition_costs(self, source_candidate, target_candidates):
        measurement, edge, location, _ = source_candidate.body
        source_measurement = measurement
        source_edge_location = (edge, location)

        # Prepare targets for routing and cost calculation
        target_measurements = []
        target_edge_locations = []
        for candidate in target_candidates:
            measurement, edge, location, _ = candidate.body
            target_measurements.append(measurement)
            target_edge_locations.append((edge, location))

        route_results = road_routing.road_network_route_many(
            source_edge_location,
            target_edge_locations,
            self.get_road_edges,
            max_path_cost=self.max_route_distance)

        costs = []
        for measurement, (_, route_distance) in zip(target_measurements, route_results):
            if route_distance < 0:
                # Not reachable
                costs.append(-1)
                continue
            # Geodesic distance based on WGS 84 spheroid
            bird_fly_distance = distance.vincenty(
                (source_measurement.lat, source_measurement.lon),
                (measurement.lat, measurement.lon)).meters
            delta = abs(route_distance - bird_fly_distance)
            costs.append(delta / self.beta)

        # # They should be equivalent (only for testing):
        # for cost, target_candidate in zip(costs, target_candidates):
        #     single_cost = self.calculate_transition_cost(source_candidate, target_candidate)
        #     assert abs(cost - single_cost) < 0.0000001

        return costs

    def calculate_emission_cost(self, candidate):
        _, _, _, distance = candidate.body
        return (distance * distance) / (self.sigma_z * self.sigma_z * 2)

    def offline_match(self, candidates):
        for winner in self.offline_search(candidates, _timestamp_key):
            yield winner

    def online_match(self, candidates):
        for winner in self.online_search(candidates, _timestamp_key):
            yield winner
