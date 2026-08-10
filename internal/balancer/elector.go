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

const bannedPenalty = math.MaxUint64

type electionEntry struct {
	key        endpoint.Key
	cumulative int64
}

type electionSnapshot struct {
	entries     []electionEntry
	totalWeight int64
	uniform     bool
	allowBanned bool
}

type electionCandidate struct {
	key     endpoint.Key
	penalty uint64
	weight  uint64
}

// endpointElector keeps mutable health penalties outside the immutable estimator tree.
// Discovery and Ban/Unban rebuild a snapshot; the request hot path only loads it atomically.
type endpointElector struct {
	mu sync.Mutex

	estimates   []strategy.Estimation
	connections map[endpoint.Key]conn.Conn
	penalties   map[endpoint.Key]uint64
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
		penalties:   make(map[endpoint.Key]uint64),
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

	if e.penalties[key] == bannedPenalty {
		return
	}
	e.penalties[key] = bannedPenalty
	e.rebuildLocked()
}

func (e *endpointElector) Unpessimize(key endpoint.Key) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.penalties[key]; !ok {
		return
	}
	delete(e.penalties, key)
	e.rebuildLocked()
}

func (e *endpointElector) rebuildLocked() {
	best, minimum := e.bestCandidates()
	snapshot := &electionSnapshot{allowBanned: len(best) > 0 && minimum == bannedPenalty}
	if len(best) == 0 {
		e.snapshot.Store(snapshot)

		return
	}

	maxWeight := uint64(math.MaxInt64 / len(best))
	snapshot.entries = make([]electionEntry, len(best))
	snapshot.uniform = true
	var firstWeight int64
	for i, candidate := range best {
		weight := int64(min(candidate.weight, maxWeight))
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

func (e *endpointElector) bestCandidates() ([]electionCandidate, uint64) {
	candidates := make([]electionCandidate, 0, len(e.estimates))
	minimum := uint64(math.MaxUint64)
	for _, estimation := range e.estimates {
		if estimation.Weight == 0 {
			continue
		}
		connection := e.connections[estimation.Key]
		if connection != nil && !isKnownConnection(connection) {
			continue
		}
		runtimePenalty := e.penalties[estimation.Key]
		if connection != nil && connection.State() == state.Banned {
			runtimePenalty = bannedPenalty
		}
		penalty := saturatingPenalty(estimation.Penalty, runtimePenalty)
		minimum = min(minimum, penalty)
		candidates = append(candidates, electionCandidate{
			key:     estimation.Key,
			penalty: penalty,
			weight:  estimation.Weight,
		})
	}

	best := make([]electionCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.penalty == minimum {
			best = append(best, candidate)
		}
	}

	return best, minimum
}

func saturatingPenalty(policy, runtime uint64) uint64 {
	if math.MaxUint64-policy < runtime {
		return math.MaxUint64
	}

	return policy + runtime
}

func isKnownConnection(connection conn.Conn) bool {
	switch connection.State() {
	case state.Created, state.Online, state.Offline, state.Banned:
		return true
	default:
		return false
	}
}
