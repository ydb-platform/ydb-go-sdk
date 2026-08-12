package balancer

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type electionSnapshot struct {
	connections    []conn.Conn
	allowBanned    bool
	candidateCount int
	hasPessimized  bool
}

// endpointElector combines immutable policy priorities with mutable connection health.
// Discovery and pessimization rebuild a snapshot; the request hot path only loads it atomically.
type endpointElector struct {
	mu sync.Mutex

	priorities     []policy.EndpointPriority
	connections    map[endpoint.Key]conn.Conn
	pessimized     map[endpoint.Key]struct{}
	rand           xrand.Rand
	preferredCount int

	snapshot atomic.Pointer[electionSnapshot]
}

func newEndpointElector(
	priorities []policy.EndpointPriority,
	connections map[endpoint.Key]conn.Conn,
	rand xrand.Rand,
) *endpointElector {
	if rand == nil {
		rand = xrand.New(xrand.WithLock())
	}
	elector := &endpointElector{
		priorities:  append([]policy.EndpointPriority(nil), priorities...),
		connections: connections,
		pessimized:  make(map[endpoint.Key]struct{}),
		rand:        rand,
	}
	minimumPriority := uint64(math.MaxUint64)
	for _, candidate := range priorities {
		if connections[candidate.Key] == nil {
			continue
		}
		switch {
		case candidate.Priority < minimumPriority:
			minimumPriority = candidate.Priority
			elector.preferredCount = 1
		case candidate.Priority == minimumPriority:
			elector.preferredCount++
		}
	}
	elector.rebuildLocked()

	return elector
}

func (e *endpointElector) Next() (connection conn.Conn, allowBanned bool, ok bool) {
	snapshot := e.snapshot.Load()
	if snapshot != nil && snapshot.hasPessimized {
		e.restoreUnbanned()
		snapshot = e.snapshot.Load()
	}
	if snapshot == nil || len(snapshot.connections) == 0 {
		return nil, false, false
	}

	connection = snapshot.connections[e.rand.Int(len(snapshot.connections))]

	return connection, snapshot.allowBanned, true
}

func (e *endpointElector) restoreUnbanned() {
	e.mu.Lock()
	defer e.mu.Unlock()

	changed := false
	for key := range e.pessimized {
		connection := e.connections[key]
		if connection != nil && isConnectionStateUsable(connection.State(), false) {
			delete(e.pessimized, key)
			changed = true
		}
	}
	if changed {
		e.rebuildLocked()
	}
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

	if e.connections[key] == nil {
		return
	}
	if _, ok := e.pessimized[key]; ok {
		return
	}
	e.pessimized[key] = struct{}{}
	e.rebuildLocked()
}

func (e *endpointElector) rebuildLocked() {
	snapshot := &electionSnapshot{
		connections:   make([]conn.Conn, 0, len(e.priorities)),
		hasPessimized: len(e.pessimized) > 0,
	}
	minimumEffective := uint64(math.MaxUint64)
	for _, candidate := range e.priorities {
		connection := e.connections[candidate.Key]
		if connection == nil {
			continue
		}

		_, pessimized := e.pessimized[candidate.Key]
		connectionState := connection.State()
		healthyCandidate := !pessimized && isConnectionStateUsable(connectionState, false)
		bannedCandidate := isConnectionStateUsable(connectionState, true) &&
			(pessimized || connectionState == state.Banned)
		effectivePriority := candidate.Priority
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
		switch {
		case effectivePriority < minimumEffective:
			minimumEffective = effectivePriority
			snapshot.connections = append(snapshot.connections[:0], connection)
			snapshot.allowBanned = allowBanned
		case effectivePriority == minimumEffective:
			snapshot.connections = append(snapshot.connections, connection)
			snapshot.allowBanned = snapshot.allowBanned || allowBanned
		}
	}

	e.snapshot.Store(snapshot)
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
