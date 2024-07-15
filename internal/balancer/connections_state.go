package balancer

import (
	"context"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type connectionsState struct {
	index map[uint32]endpoint.Endpoint

	checkEndpoint func(endpoint.Endpoint) bool

	prefer   []endpoint.Endpoint
	fallback []endpoint.Endpoint
	all      []endpoint.Endpoint

	rand xrand.Rand
}

func newConnectionsState(
	endpoints []endpoint.Endpoint,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
	checkEndpoint func(endpoint.Endpoint) bool,
) *connectionsState {
	s := &connectionsState{
		index:         endpointsToNodeIDMap(endpoints),
		rand:          xrand.New(xrand.WithLock()),
		checkEndpoint: checkEndpoint,
	}

	s.prefer, s.fallback = sortPreferEndpoints(endpoints, filter, info, allowFallback)
	if allowFallback {
		s.all = endpoints
	} else {
		s.all = s.prefer
	}

	return s
}

func (s *connectionsState) PreferredCount() int {
	return len(s.prefer)
}

func (s *connectionsState) All() []endpoint.Endpoint {
	if s == nil {
		return nil
	}

	return s.all
}

func (s *connectionsState) Next(ctx context.Context) endpoint.Endpoint {
	if err := ctx.Err(); err != nil {
		return nil
	}

	if e := s.preferEndpoint(ctx); e != nil {
		return e
	}

	if e := s.selectRandomEndpoint(s.prefer); e != nil {
		return e
	}

	if e := s.selectRandomEndpoint(s.fallback); e != nil {
		return e
	}

	return s.selectRandomEndpoint(s.all)
}

func (s *connectionsState) preferEndpoint(ctx context.Context) endpoint.Endpoint {
	if nodeID, has := endpoint.ContextNodeID(ctx); has {
		e := s.index[nodeID]
		if e != nil && s.checkEndpoint(e) {
			return e
		}
	}

	return nil
}

func (s *connectionsState) selectRandomEndpoint(endpoints []endpoint.Endpoint) endpoint.Endpoint {
	count := len(endpoints)
	if count == 0 {
		// return for empty list need for prevent panic in fast path
		return nil
	}

	// fast path
	if e := endpoints[s.rand.Int(count)]; s.checkEndpoint(e) {
		return e
	}

	// shuffled indexes slices need for guarantee about every connection will check
	indexes := make([]int, count)
	for index := range indexes {
		indexes[index] = index
	}

	s.rand.Shuffle(count, func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})

	for _, index := range indexes {
		e := endpoints[index]
		if s.checkEndpoint(e) {
			return e
		}
	}

	return nil
}

func excludeS(in []endpoint.Endpoint, exclude endpoint.Endpoint) (out []endpoint.Endpoint) {
	out = make([]endpoint.Endpoint, 0, len(in))

	for i := range in {
		if in[i].Address() != exclude.Address() {
			out = append(out, in[i])
		}
	}

	return out
}

func excludeM(in map[uint32]endpoint.Endpoint, exclude endpoint.Endpoint) (out map[uint32]endpoint.Endpoint) {
	out = make(map[uint32]endpoint.Endpoint, len(in))

	for i := range in {
		if in[i].Address() != exclude.Address() {
			out[in[i].NodeID()] = in[i]
		}
	}

	return out
}

func (s *connectionsState) exclude(e endpoint.Endpoint) *connectionsState {
	return &connectionsState{
		index:         excludeM(s.index, e),
		checkEndpoint: s.checkEndpoint,
		prefer:        excludeS(s.prefer, e),
		fallback:      excludeS(s.fallback, e),
		all:           excludeS(s.all, e),
		rand:          s.rand,
	}
}

func endpointsToNodeIDMap(endpoints []endpoint.Endpoint) (index map[uint32]endpoint.Endpoint) {
	if len(endpoints) == 0 {
		return nil
	}
	index = make(map[uint32]endpoint.Endpoint, len(endpoints))
	for _, c := range endpoints {
		index[c.NodeID()] = c
	}

	return index
}

func sortPreferEndpoints(
	endpoints []endpoint.Endpoint,
	filter balancerConfig.Filter,
	info balancerConfig.Info,
	allowFallback bool,
) (prefer, fallback []endpoint.Endpoint) {
	if filter == nil {
		return endpoints, nil
	}

	prefer = make([]endpoint.Endpoint, 0, len(endpoints))
	if allowFallback {
		fallback = make([]endpoint.Endpoint, 0, len(endpoints))
	}

	for _, c := range endpoints {
		if filter.Allow(info, c) {
			prefer = append(prefer, c)
		} else if allowFallback {
			fallback = append(fallback, c)
		}
	}

	return prefer, fallback
}
