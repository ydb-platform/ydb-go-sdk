package balancer

import (
	"context"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type (
	connectionsSlice[T conn.Info] []T
	connections[T conn.Info]      struct {
		connByNodeID map[uint32]T

		prefer   []T
		fallback []T
		all      []T

		rand xrand.Rand
	}
)

func newConnections[T conn.Info](
	conns []T,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
) (s *connections[T]) {
	s = &connections[T]{
		connByNodeID: connsToNodeIDMap(conns),
		rand:         xrand.New(xrand.WithLock()),
	}

	s.prefer, s.fallback = sortPreferConnections(conns, filter, info, allowFallback)
	if allowFallback {
		s.all = conns
	} else {
		s.all = s.prefer
	}

	return s
}

func (s *connections[T]) withBadConn(c T) (ss *connections[T], changed bool) {
	ss = &connections[T]{
		connByNodeID: s.connByNodeID,
		rand:         s.rand,
		prefer:       make(connectionsSlice[T], 0, len(s.prefer)),
		fallback:     make(connectionsSlice[T], 0, len(s.fallback)+1),
		all:          s.all,
	}

	for _, cc := range s.prefer {
		if !endpoint.Equals(c.Endpoint(), cc.Endpoint()) {
			ss.prefer = append(ss.prefer, cc)
		} else {
			changed = true
			ss.fallback = append(ss.fallback, c)
		}
	}
	ss.fallback = append(ss.fallback, s.fallback...)

	return ss, changed
}

func (s *connections[T]) PreferredCount() int {
	return len(s.prefer)
}

func (s *connections[T]) GetConn(ctx context.Context) (nilConn T, failedCount int) {
	if err := ctx.Err(); err != nil {
		return nilConn, 0
	}

	if cc, has := s.preferConnection(ctx); has {
		return cc, 0
	}

	cc, tryCount, has := selectRandomConnection(s.rand, s.prefer, false)
	failedCount += tryCount
	if has {
		return cc, failedCount
	}

	cc, tryCount, has = selectRandomConnection(s.rand, s.fallback, false)
	failedCount += tryCount
	if has {
		return cc, failedCount
	}

	cc, tryCount, has = selectRandomConnection(s.rand, s.all, true)
	failedCount += tryCount
	if has {
		return cc, failedCount
	}

	return nilConn, failedCount
}

func (s *connections[T]) preferConnection(ctx context.Context) (nilConn T, has bool) {
	if e, hasPreferEndpoint := ContextEndpoint(ctx); hasPreferEndpoint {
		cc, ok := s.connByNodeID[e.NodeID()]
		if ok && cc.Ready() {
			return cc, true
		}
	}

	return nilConn, false
}

func selectRandomConnection[T conn.Info](
	r xrand.Rand, conns []T, notReadyIsOk bool,
) (nilConn T, failedConns int, has bool) {
	connCount := len(conns)
	if connCount == 0 {
		// return for empty list need for prevent panic in fast path
		return nilConn, 0, false
	}

	// fast path
	if cc := conns[r.Int(connCount)]; cc.Ready() {
		return cc, 0, true
	}

	// shuffled indexes slices need for guarantee about every connection will check
	indexes := make([]int, connCount)
	for index := range indexes {
		indexes[index] = index
	}
	r.Shuffle(connCount, func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})

	for _, index := range indexes {
		if cc := conns[index]; notReadyIsOk || cc.Ready() {
			return cc, 0, true
		}
		failedConns++
	}

	return nilConn, failedConns, false
}

func connsToNodeIDMap[T conn.Info](conns []T) (nodes map[uint32]T) {
	if len(conns) == 0 {
		return nil
	}
	nodes = make(map[uint32]T, len(conns))
	for _, c := range conns {
		nodes[c.Endpoint().NodeID()] = c
	}

	return nodes
}

func sortPreferConnections[T conn.Info](
	conns []T,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
) (prefer, fallback []T) {
	if filter == nil {
		return conns, nil
	}

	prefer = make([]T, 0, len(conns))
	if allowFallback {
		fallback = make([]T, 0, len(conns))
	}

	for _, c := range conns {
		if filter.Allow(info, c) {
			prefer = append(prefer, c)
		} else if allowFallback {
			fallback = append(fallback, c)
		}
	}

	return prefer, fallback
}
