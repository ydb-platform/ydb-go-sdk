package cluster

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xslices"
)

type (
	Cluster struct {
		filter        func(e endpoint.Info) bool
		allowFallback bool

		index map[uint32]endpoint.Endpoint

		prefer   []endpoint.Endpoint
		fallback []endpoint.Endpoint
		all      []endpoint.Endpoint

		rand xrand.Rand
	}
	option func(s *Cluster)
)

func WithFilter(filter func(e endpoint.Info) bool) option {
	return func(s *Cluster) {
		s.filter = filter
	}
}

func WithFallback(allowFallback bool) option {
	return func(s *Cluster) {
		s.allowFallback = allowFallback
	}
}

func New(endpoints []endpoint.Endpoint, opts ...option) *Cluster {
	s := &Cluster{
		filter: func(e endpoint.Info) bool {
			return true
		},
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.rand == nil {
		s.rand = xrand.New(xrand.WithLock())
	}

	s.prefer, s.fallback = xslices.Split(endpoints, func(e endpoint.Endpoint) bool {
		return s.filter(e)
	})

	if s.allowFallback {
		s.all = endpoints
		s.index = xslices.Map(endpoints, func(e endpoint.Endpoint) uint32 { return e.NodeID() })
	} else {
		s.all = s.prefer
		s.fallback = nil
		s.index = xslices.Map(s.prefer, func(e endpoint.Endpoint) uint32 { return e.NodeID() })
	}

	return s
}

func (s *Cluster) All() (all []endpoint.Endpoint) {
	if s == nil {
		return nil
	}

	return s.all
}

func Without(s *Cluster, endpoints ...endpoint.Endpoint) *Cluster {
	prefer := make([]endpoint.Endpoint, 0, len(s.prefer))
	fallback := s.fallback
	for _, endpoint := range endpoints {
		for i := range s.prefer {
			if s.prefer[i].Address() != endpoint.Address() {
				prefer = append(prefer, s.prefer[i])
			} else {
				fallback = append(fallback, s.prefer[i])
			}
		}
	}

	return &Cluster{
		filter:        s.filter,
		allowFallback: s.allowFallback,
		index:         s.index,
		prefer:        prefer,
		fallback:      fallback,
		all:           s.all,
		rand:          s.rand,
	}
}

func (s *Cluster) Next(ctx context.Context) (endpoint.Endpoint, error) {
	if s == nil {
		return nil, ErrNilPtr
	}

	if err := ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if nodeID, wantEndpointByNodeID := endpoint.ContextNodeID(ctx); wantEndpointByNodeID {
		e, has := s.index[nodeID]
		if has {
			return e, nil
		}
	}

	if l := len(s.prefer); l > 0 {
		return s.prefer[s.rand.Int(l)], nil
	}

	if l := len(s.fallback); l > 0 {
		return s.fallback[s.rand.Int(l)], nil
	}

	return nil, xerrors.WithStackTrace(ErrNoEndpoints)
}
