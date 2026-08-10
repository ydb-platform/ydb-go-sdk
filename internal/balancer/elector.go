package balancer

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type electionEntry struct {
	key        endpoint.Key
	cumulative int64
}

type electionSnapshot struct {
	entries     []electionEntry
	totalWeight int64
	uniform     bool
	allowBanned bool

	preferredCount       int
	unavailablePreferred int
}

type electionCandidate struct {
	key     endpoint.Key
	penalty uint64
	weight  uint64
}

// endpointElector keeps mutable health state outside the immutable estimator tree.
// Discovery and pessimization rebuild a snapshot; the request hot path only loads it atomically.
type endpointElector struct {
	mu sync.Mutex

	estimates   []strategy.Estimation
	connections map[endpoint.Key]conn.Conn
	pessimized  map[endpoint.Key]struct{}
	rand        xrand.Rand

	snapshot atomic.Pointer[electionSnapshot]
}

func newEndpointElector(
	estimates []strategy.Estimation,
	connections map[endpoint.Key]conn.Conn,
	rand xrand.Rand,
) *endpointElector {
	if rand == nil {
		rand = xrand.New(xrand.WithLock())
	}
	elector := &endpointElector{
		estimates:   append([]strategy.Estimation(nil), estimates...),
		connections: connections,
		pessimized:  make(map[endpoint.Key]struct{}),
		rand:        rand,
	}
	elector.rebuildLocked()

	return elector
}

func (e *endpointElector) Next() (key endpoint.Key, allowBanned bool, ok bool) {
	snapshot := e.snapshot.Load()
	if snapshot == nil || len(snapshot.entries) == 0 {
		return endpoint.Key{}, false, false
	}

	var index int
	if snapshot.uniform {
		index = e.rand.Int(len(snapshot.entries))
	} else {
		target := e.rand.Int64(snapshot.totalWeight) + 1
		index = sort.Search(len(snapshot.entries), func(i int) bool {
			return snapshot.entries[i].cumulative >= target
		})
	}

	return snapshot.entries[index].key, snapshot.allowBanned, true
}

func (e *endpointElector) Pessimize(key endpoint.Key) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.pessimized[key]; ok {
		return
	}
	e.pessimized[key] = struct{}{}
	e.rebuildLocked()
}

func (e *endpointElector) PreferenceHealth() (preferred, unavailable int) {
	snapshot := e.snapshot.Load()
	if snapshot == nil {
		return 0, 0
	}

	return snapshot.preferredCount, snapshot.unavailablePreferred
}

func (e *endpointElector) rebuildLocked() {
	best, allowBanned, preferredCount, unavailablePreferred := e.bestCandidates()
	snapshot := &electionSnapshot{
		allowBanned:          allowBanned,
		preferredCount:       preferredCount,
		unavailablePreferred: unavailablePreferred,
	}
	if len(best) == 0 {
		e.snapshot.Store(snapshot)

		return
	}

	weights := normalizeElectionWeights(best)
	snapshot.entries = make([]electionEntry, len(best))
	snapshot.uniform = true
	var firstWeight int64
	for i, candidate := range best {
		weight := int64(weights[i])
		if i == 0 {
			firstWeight = weight
		} else if weight != firstWeight {
			snapshot.uniform = false
		}
		snapshot.totalWeight += weight
		snapshot.entries[i] = electionEntry{
			key:        candidate.key,
			cumulative: snapshot.totalWeight,
		}
	}
	e.snapshot.Store(snapshot)
}

func (e *endpointElector) bestCandidates() (
	best []electionCandidate,
	allowBanned bool,
	preferredCount int,
	unavailablePreferred int,
) {
	healthy, banned, preferredCount, unavailablePreferred := e.candidatesByHealth()
	if len(healthy) == 0 {
		return banned, len(banned) > 0, preferredCount, unavailablePreferred
	}

	minimumHealthyPenalty := uint64(math.MaxUint64)
	for _, candidate := range healthy {
		minimumHealthyPenalty = min(minimumHealthyPenalty, candidate.penalty)
	}
	best = make([]electionCandidate, 0, len(healthy))
	for _, candidate := range healthy {
		if candidate.penalty == minimumHealthyPenalty {
			best = append(best, candidate)
		}
	}

	return best, false, preferredCount, unavailablePreferred
}

func (e *endpointElector) candidatesByHealth() (
	healthy []electionCandidate,
	banned []electionCandidate,
	preferredCount int,
	unavailablePreferred int,
) {
	minimumPolicy := e.minimumPolicyPenalty()
	healthy = make([]electionCandidate, 0, len(e.estimates))
	banned = make([]electionCandidate, 0, len(e.estimates))
	for _, estimation := range e.estimates {
		if estimation.Weight == 0 {
			continue
		}
		connection := e.connections[estimation.Key]
		if connection == nil {
			continue
		}

		_, pessimized := e.pessimized[estimation.Key]
		connectionState := connection.State()
		healthyCandidate := !pessimized && isHealthyConnectionState(connectionState)
		bannedCandidate := pessimized || connectionState == state.Banned
		if estimation.Penalty == minimumPolicy {
			preferredCount++
			if !healthyCandidate {
				unavailablePreferred++
			}
		}

		candidate := electionCandidate{
			key:     estimation.Key,
			penalty: estimation.Penalty,
			weight:  estimation.Weight,
		}
		switch {
		case healthyCandidate:
			healthy = append(healthy, candidate)
		case bannedCandidate:
			banned = append(banned, candidate)
		}
	}

	return healthy, banned, preferredCount, unavailablePreferred
}

func (e *endpointElector) minimumPolicyPenalty() uint64 {
	minimumPolicy := uint64(math.MaxUint64)
	for _, estimation := range e.estimates {
		if estimation.Weight != 0 && e.connections[estimation.Key] != nil {
			minimumPolicy = min(minimumPolicy, estimation.Penalty)
		}
	}

	return minimumPolicy
}

func normalizeElectionWeights(candidates []electionCandidate) []uint64 {
	weights := make([]uint64, len(candidates))
	if len(candidates) == 0 {
		return weights
	}

	maximumPerCandidate := uint64(math.MaxInt64 / len(candidates))
	var maximum uint64
	for _, candidate := range candidates {
		maximum = max(maximum, candidate.weight)
	}
	scale := maximum / maximumPerCandidate
	if maximum%maximumPerCandidate != 0 {
		scale++
	}
	for i, candidate := range candidates {
		weights[i] = max(uint64(1), candidate.weight/scale)
	}

	return weights
}

func isHealthyConnectionState(connectionState state.State) bool {
	switch connectionState {
	case state.Created, state.Online, state.Offline:
		return true
	default:
		return false
	}
}
