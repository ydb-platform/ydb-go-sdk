package balancer

import (
	"math"
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
	majorityBanned bool
}

// endpointElector combines immutable policy priorities with connection health snapshots.
type endpointElector struct {
	priorities  []policy.EndpointPriority
	connections map[endpoint.Key]conn.Conn
	rand        xrand.Rand

	snapshot atomic.Pointer[electionSnapshot]
}

// newEndpointElector takes ownership of priorities and connections.
func newEndpointElector(
	priorities []policy.EndpointPriority,
	connections map[endpoint.Key]conn.Conn,
	rand xrand.Rand,
) *endpointElector {
	if rand == nil {
		rand = xrand.New(xrand.WithLock())
	}
	elector := &endpointElector{
		priorities:  priorities,
		connections: connections,
		rand:        rand,
	}
	elector.Refresh()

	return elector
}

func (e *endpointElector) Next() (connection conn.Conn, allowBanned bool, ok bool) {
	snapshot := e.snapshot.Load()
	if snapshot == nil || len(snapshot.connections) == 0 {
		return nil, false, false
	}

	connectionCount := len(snapshot.connections)
	if connectionCount == 1 {
		return snapshot.connections[0], snapshot.allowBanned, true
	}
	first := e.rand.Int(connectionCount)
	second := e.rand.Int(connectionCount - 1)
	if second >= first {
		second++
	}
	connection = snapshot.connections[first]
	if other := snapshot.connections[second]; other.InFlight() < connection.InFlight() {
		connection = other
	}

	return connection, snapshot.allowBanned, true
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

// Refresh atomically publishes a new election snapshot derived from conn.State().
// Concurrent refreshes may overwrite each other, but a stale selected connection is
// checked by nextAvailableConn and causes another refresh.
// It reports whether the share of banned records has just crossed 50%.
func (e *endpointElector) Refresh() (forceDiscovery bool) {
	if e == nil {
		return false
	}

	previous := e.snapshot.Load()
	snapshot := &electionSnapshot{
		connections: make([]conn.Conn, 0, len(e.priorities)),
	}
	banned := make([]conn.Conn, 0, len(e.priorities))
	minimumHealthyPriority := uint64(math.MaxUint64)
	recordCount := 0
	for _, candidate := range e.priorities {
		if candidate.Excluded {
			continue
		}
		connection := e.connections[candidate.Key]
		if connection == nil {
			continue
		}
		recordCount++
		connectionState := connection.State()
		switch {
		case isConnectionStateUsable(connectionState, false):
			snapshot.candidateCount++
			switch {
			case candidate.Priority < minimumHealthyPriority:
				minimumHealthyPriority = candidate.Priority
				snapshot.connections = append(snapshot.connections[:0], connection)
			case candidate.Priority == minimumHealthyPriority:
				snapshot.connections = append(snapshot.connections, connection)
			}
		case connectionState == state.Banned:
			snapshot.candidateCount++
			banned = append(banned, connection)
		default:
			// Unknown and destroyed connections are intentionally excluded from election.
		}
	}
	if len(snapshot.connections) == 0 && len(banned) > 0 {
		snapshot.connections = banned
		snapshot.allowBanned = true
	}

	snapshot.majorityBanned = len(banned) > recordCount/2
	e.snapshot.Store(snapshot)

	return snapshot.majorityBanned && (previous == nil || !previous.majorityBanned)
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
