import collections
import itertools
import heapq

try:
    from itertools import (
        izip as zip,
        imap as map,
        ifilter as filter)
except ImportError:
    pass


# A wrapper of candidate that is needed by viterbi search. The real
# candidate is stored in the body attribute. All candidate objects in
# this module refer to this wrapper.
CandidateWrapper = collections.namedtuple('Candidate', ['id', 'timestamp', 'body'])


def _pop_unscanned_candidate(pqueue, scanned):
    """
    Pop out the first unscanned candidate in the pqueue. Return a
    tuple of Nones if no more unscanned candidates found.
    """
    if not pqueue:
        return None, None, None
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


def _wrap_candidates(raw_candidates):
    """
    Group the raw candidate iterable by each candidate's group key,
    and attach id and timestamp to each wrapped candidate. Create a
    generator which returns groups of wrapped candidates.
    """
    # The raw candidates should be in order:
    # assert raw_candidates == sorted(raw_candidates, key=lambda c: c.group_key)
    # We don't sort it here is because it's possible to be an iterator
    groups = itertools.groupby(raw_candidates, key=lambda c: c.group_key)

    # Attach index as ID for each candidate. The ID will be used to
    # identify candidate during viterbi path search. Do this just
    # because the candidate object probably is not hashable
    id = itertools.count()
    for timestamp, (_, candidates) in enumerate(groups):
        yield [CandidateWrapper(id=next(id), timestamp=timestamp, body=candidate)
               for candidate in candidates]


class IndexedIterator(list):
    """
    A combination object of list and iterator. The list caches all
    items fetched from the iterable.
    """
    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.buffer = []
        super(IndexedIterator, self).__init__()

    def next(self):
        if self.buffer:
            item = self.buffer.pop()
        else:
            item = next(self.iterator)
        # Cache it
        self.append(item)
        return item

    def __iter__(self):
        return self

    def push_back(self):
        """Push the recently fetched item back to the iterable."""
        self.buffer.append(self.pop())


def test_indexed_iterator():
    it = IndexedIterator(range(10))
    # It should work as an iterator
    assert next(it) == 0
    assert 0 in it
    assert next(it) == 1
    assert 1 in it
    assert next(it) == 2
    assert 2 in it
    # It should have cached all iterated items
    assert it == [0, 1, 2]
    # It should push back 2 items from the list to the buffer
    it.push_back()
    it.push_back()
    assert it == [0]
    assert next(it) == 1
    assert it == [0, 1]
    assert next(it) == 2
    assert it == [0, 1, 2]
    # It should work as a list
    for i, n in enumerate(it):
        assert n == i + 3
    for i in range(10):
        assert i in it


class ViterbiSearch(object):
    """
    A base class that finds the optimal viterbi path in a heuristic
    way.
    """

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

    def _start(self, state):
        """
        Start searching from the state. Return a priority queue with all
        candidates in this state loaded.
        """
        pqueue = [(self.calculate_emission_cost(candidate.body), candidate, None)
                  for candidate in state]
        heapq.heapify(pqueue)
        return pqueue

    def search_winners(self, states):
        """
        Search for the winner (the best candidate) at each state. It
        returns a generator that generates, at each state, a winner, a
        scanned table used to reconstruct the path, and a bool flag to
        indicate if this winner a new start or not. A new start means
        there is no way from previous state to current winner.

        `states` should be iterable.
        """
        pqueue = []
        winner = None
        scanned_candidates = {}

        while True:
            if winner is None:
                assert not scanned_candidates
            else:
                assert scanned_candidates
                latest_timestamp = len(states) - 1
                assert latest_timestamp >= 1
                assert winner.timestamp + 1 == latest_timestamp

            cost_sofar, candidate, prev_candidate = _pop_unscanned_candidate(pqueue, scanned_candidates)

            if candidate is None:
                # A new start
                start_state = next(states) if winner is None else states[-1]
                pqueue = self._start(start_state)
                winner = None
                scanned_candidates = {}
                continue

            scanned_candidates[candidate.id] = prev_candidate
            timestamp = candidate.timestamp

            if winner is None or winner.timestamp < timestamp:
                new_start = winner is None
                winner = candidate
                yield winner, scanned_candidates, new_start

            # Remove the scanned candidate from current state
            state = states[timestamp]
            state.remove(candidate)
            # If current state has no candidates (all candidates
            # scanned), then remove all earlier candidates in the
            # queue, since it is impossible for them to reach current
            # state with lower costs
            if not state:
                pqueue = list(filter(lambda c: c[1].timestamp > timestamp, pqueue))
                heapq.heapify(pqueue)

            # Next state is not always the latest
            if timestamp + 1 < len(states):
                next_state = states[timestamp + 1]
            else:
                next_state = next(states)

            transition_costs = self.calculate_transition_costs(
                candidate.body, [c.body for c in next_state])
            assert len(transition_costs) == len(next_state)
            for next_candidate, transition_cost in zip(next_state, transition_costs):
                assert next_candidate.id not in scanned_candidates, \
                    'Scanned candidates should have been removed from the state earlier'
                # Skip any negative cost, which means the candidate is unreachable
                if transition_cost < 0:
                    continue
                emission_cost = self.calculate_emission_cost(next_candidate.body)
                if emission_cost < 0:
                    continue
                next_cost_sofar = cost_sofar + transition_cost + emission_cost
                heapq.heappush(pqueue, (next_cost_sofar, next_candidate, candidate))

    # Offline search guarantees the global optimum (the best path).
    # The knowledge about all candidates is needed
    def offline_search(self, candidates):
        """
        Search for the best path among `candidates`. Candidates will be
        grouped into states by each candidate's group key.

        It generates a path, a sequence of winners that are chosen
        from their states respectively.
        """
        groups = _wrap_candidates(candidates)
        states = IndexedIterator(groups)
        last_winner = None
        for winner, scanned_candidates, new_start in self.search_winners(states):
            # If it is a new start, generate the path segment ending
            # with last winner
            if last_winner and new_start:
                path = _reconstruct_path(last_winner, last_scanned_candidates)
                for candidate in reversed(path):
                    yield candidate.body
            last_winner = winner
            last_scanned_candidates = scanned_candidates

        # Don't forget the last path segment
        if last_winner:
            path = _reconstruct_path(last_winner, last_scanned_candidates)
            for candidate in reversed(path):
                yield candidate.body

    # Online search only guarantees the local optimum (the winner at
    # current state). The knowledge about the furture candidates is
    # not needed
    def online_search(self, candidates):
        """
        Search and generate the best candidate (the winner) for each
        state.
        """
        groups = _wrap_candidates(candidates)
        states = IndexedIterator(groups)
        for winner, _, _ in self.search_winners(states):
            yield winner.body


# Theoretically this naive viterbi search is slower than the
# implementation of ViterbiSearch above. We put it here just for
# comparision and testing.
class NaiveViterbiSearch(ViterbiSearch):
    """
    The naive viterbi algorithm:
    https://en.wikipedia.org/wiki/Viterbi_algorithm
    """
    def search_winners(self, states):
        # A belief state is a list of tuples (prob, candidate,
        # previous candidate) to describe the probablitity of each
        # candidate in the state at a certain time. The previous
        # candidates are used to reconstruct the most likely path
        prev_belief_state = []
        scanned_candidates = {}

        for state in states:
            if not state:
                continue

            belief_state = [(0, c, None) for c in state]
            state_size = len(belief_state)
            for prev_prob, prev_candidate, _ in prev_belief_state:
                if prev_prob <= 0:
                    continue
                transition_probs = self.calculate_transition_costs(prev_candidate.body, [c.body for c in state])
                assert len(transition_probs) == state_size
                emission_probs = list(map(self.calculate_emission_cost, [c.body for c in state]))

                # Update current belief state
                for idx in range(state_size):
                    transition_prob = transition_probs[idx]
                    emission_prob = emission_probs[idx]
                    if emission_prob <= 0 or transition_prob <= 0:
                        continue
                    new_prob = prev_prob * transition_prob * emission_prob
                    prob, _, _ = belief_state[idx]
                    if prob < new_prob:
                        belief_state[idx] = (new_prob, state[idx], prev_candidate)

            most_prob, winner, _ = max(belief_state, key=lambda c: c[0])

            # If no one at previous state can reach current state,
            # then it is a new start
            new_start = most_prob <= 0
            if new_start:
                # Update current belief state
                belief_state = [(self.calculate_emission_cost(c.body), c, None) for c in state]
                scanned_candidates = {}
                most_prob, winner, _ = max(belief_state, key=lambda c: c[0])
            if most_prob <= 0:
                continue

            # Update scanned table for reconstructing path
            scanned_candidates.update({c.id: pc for _, c, pc in belief_state})

            yield winner, scanned_candidates, new_start

            # Avoid underflow: multiplying all probability values by
            # an estimated scalar
            least_prob, _, _ = min(filter(lambda c: c[0] > 0, belief_state), key=lambda c: c[0])
            scalar = 1
            prob = least_prob
            while prob < 1:
                scalar *= 10
                prob = least_prob * scalar
            if scalar > 1:
                belief_state = [(p * scalar, c, pc) for p, c, pc in belief_state]

            prev_belief_state = belief_state
