package balancer

import (
	"math"
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
	connection conn.Conn
}

type electionSnapshot struct {
	entries        []electionEntry
	allowBanned    bool
	candidateCount int

	preferredCount       int
	unavailablePreferred int
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

func (e *endpointElector) Next() (key endpoint.Key, connection conn.Conn, allowBanned bool, ok bool) {
	snapshot := e.snapshot.Load()
	if snapshot == nil || len(snapshot.entries) == 0 {
		return endpoint.Key{}, nil, false, false
	}

	index := e.rand.Int(len(snapshot.entries))
	entry := snapshot.entries[index]

	return entry.key, entry.connection, snapshot.allowBanned, true
}

func (e *endpointElector) CandidateCount() int {
	if e == nil {
		return 0
	}

	snapshot := e.snapshot.Load()
	if snapshot == nil {
		return 0
	}

	return snapshot.candidateCount
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
	snapshot := &electionSnapshot{
		entries: make([]electionEntry, 0, len(e.estimates)),
	}
	minimumPolicy := e.minimumPolicyPriority()
	minimumEffective := uint64(math.MaxUint64)
	for _, estimation := range e.estimates {
		connection := e.connections[estimation.Key]
		if connection == nil {
			continue
		}

		_, pessimized := e.pessimized[estimation.Key]
		connectionState := connection.State()
		healthyCandidate := !pessimized && isConnectionStateUsable(connectionState, false)
		bannedCandidate := isConnectionStateUsable(connectionState, true) &&
			(pessimized || connectionState == state.Banned)
		if estimation.Priority == minimumPolicy {
			snapshot.preferredCount++
			if !healthyCandidate {
				snapshot.unavailablePreferred++
			}
		}

		effectivePriority := estimation.Priority
		allowBanned := false
		switch {
		case healthyCandidate:
		case bannedCandidate:
			effectivePriority = math.MaxUint64
			allowBanned = true
		default:
			// Unknown and destroyed connections are intentionally excluded from election.
			continue
		}

		snapshot.candidateCount++
		entry := electionEntry{key: estimation.Key, connection: connection}
		switch {
		case effectivePriority < minimumEffective:
			minimumEffective = effectivePriority
			snapshot.entries = append(snapshot.entries[:0], entry)
			snapshot.allowBanned = allowBanned
		case effectivePriority == minimumEffective:
			snapshot.entries = append(snapshot.entries, entry)
			snapshot.allowBanned = snapshot.allowBanned || allowBanned
		}
	}

	e.snapshot.Store(snapshot)
}

func (e *endpointElector) minimumPolicyPriority() uint64 {
	minimumPolicy := uint64(math.MaxUint64)
	for _, estimation := range e.estimates {
		if e.connections[estimation.Key] != nil {
			minimumPolicy = min(minimumPolicy, estimation.Priority)
		}
	}

	return minimumPolicy
}

func isConnectionStateUsable(connectionState state.State, allowBanned bool) bool {
	switch connectionState {
	case state.Created, state.Online, state.Offline:
		return true
	case state.Banned:
		return allowBanned
	default:
		return false
	}
}
