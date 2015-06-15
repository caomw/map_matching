import collections
import itertools
import heapq


Candidate = collections.namedtuple('Candidate', ['id', 'timestamp', 'body'])


def _pop_unscanned_candidate(pqueue, scanned):
    """
    Pop out the first unscanned candidate in the pqueue. Return a
    tuple of Nones if no more unscanned candidates found.
    """
    assert pqueue
    while True:
        cost_sofar, candidate, prev_candidate = heapq.heappop(pqueue)
        if not pqueue or candidate.id not in scanned:
            break
    if candidate.id in scanned:
        assert not pqueue
        return None, None, None
    return cost_sofar, candidate, prev_candidate


def _reconstruct_path(target_candidate, scanned):
    """
    Reconstruct a path from the scanned table, and return the path
    sequence.
    """
    path = []
    candidate = target_candidate
    while candidate is not None:
        path.append(candidate)
        candidate = scanned[candidate.id]
    return path


class ViterbiSearch(object):
    """
    A base class that finds the optimal viterbi path in a heuristic
    way.
    """
    def __init__(self, candidate_bodies, timestamp_key=None):
        sorted_candidate_bodies = sorted(candidate_bodies, key=timestamp_key)
        groups = itertools.groupby(sorted_candidate_bodies, key=timestamp_key)

        # Attach index as ID for each candidate. The ID will be used
        # to identify candidate during viterbi path search. Do this
        # just in case that the candidate is not hashable
        id = itertools.count()
        self.states = [tuple(Candidate(id=next(id), timestamp=timestamp, body=body) for body in bodies)
                       for timestamp, (key, bodies) in enumerate(groups)]
        # Now self.states[t] returns all candidates at time t

    def calculate_emission_cost(self, candidate):
        """Should return emission cost of the candidate."""
        raise NotImplementedError()

    def calculate_transition_cost(self, source_candidate, target_candidate):
        """
        Should return transition cost from the source candidate to
        the target candidate.
        """
        raise NotImplementedError()

    def calculate_transition_costs(self, source_candidate, target_candidates):
        """
        Return a list of transition costs from the source candidate to
        a list of target candidates respectively.
        """
        return [self.calculate_transition_cost(source_candidate, target_candidate)
                for target_candidate in target_candidates]

    def find_best_path_since(self, start_time):
        """
        Find the best path (a sequence of candidates) since `start_time`.
        """
        scanned_candidates = {}

        # Furthest scanned candidate with best cost
        furthest_best_candidate = None
        furthest_best_cost_sofar = None

        assert self.states[start_time]
        pqueue = [(self.calculate_emission_cost(candidate), candidate, None)
                  for candidate in self.states[start_time]]
        heapq.heapify(pqueue)

        while pqueue:
            cost_sofar, candidate, prev_candidate = _pop_unscanned_candidate(pqueue, scanned_candidates)

            # No more candidates from the queue. No worry, break the
            # loop, there is a way
            if candidate is None:
                break

            scanned_candidates[candidate.id] = prev_candidate
            timestamp = candidate.timestamp

            # Update the furthest best candidate and cost
            if furthest_best_candidate is None:
                assert furthest_best_cost_sofar is None
                furthest_best_candidate = candidate
                furthest_best_cost_sofar = cost_sofar
            else:
                assert furthest_best_cost_sofar is not None
                if furthest_best_candidate.timestamp < timestamp:
                    furthest_best_candidate = candidate
                    furthest_best_cost_sofar = cost_sofar
                elif furthest_best_candidate.timestamp == timestamp:
                    assert furthest_best_cost_sofar <= cost_sofar

            # If current state has no unscanned candidates (all
            # candidates scanned), then remove all earlier candidates
            # in the queue, since it is impossible for them to reach
            # current state with lower costs
            self.unscanned_counts[timestamp] -= 1
            assert self.unscanned_counts[timestamp] >= 0
            if self.unscanned_counts[timestamp] == 0:
                if timestamp == furthest_best_candidate.timestamp:
                    pqueue = []
                else:
                    pqueue = list(filter(lambda c: c[1].timestamp > timestamp, pqueue))
                    heapq.heapify(pqueue)

            if timestamp + 1 < len(self.states):
                next_state = self.states[timestamp + 1]
            else:
                # Reach the end of states
                assert candidate == furthest_best_candidate, 'Sure you are the best!'
                break

            # Calculating transition costs to next state could be
            # expensive, so prior to that, we filter out processed or
            # unnecessary candidates from the next state
            next_candidates = list(filter(lambda c: c not in scanned_candidates, next_state))

            transition_costs = self.calculate_transition_costs(candidate, next_candidates)
            assert len(transition_costs) == len(next_candidates)
            for next_candidate, transition_cost in zip(next_candidates, transition_costs):
                # Skip any negative cost, which means the candidate is unreachable
                if transition_cost < 0:
                    continue
                emission_cost = self.calculate_emission_cost(next_candidate)
                if emission_cost < 0:
                    continue
                future_cost_sofar = cost_sofar + transition_cost + emission_cost
                heapq.heappush(pqueue, (future_cost_sofar, next_candidate, candidate))

        assert furthest_best_candidate
        return _reconstruct_path(furthest_best_candidate, scanned_candidates)

    def find_best_paths(self):
        self.unscanned_counts = [len(candidates) for candidates in self.states]
        start_time = 0
        paths = []
        while start_time < len(self.states):
            path = self.find_best_path_since(start_time)
            paths.append(path)
            end_time = path[0].timestamp
            assert end_time >= start_time
            # A new start
            start_time = end_time + 1
        return paths
