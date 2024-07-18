package state

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xslices"
)

type (
	state struct {
		filter        func(e endpoint.Info) bool
		allowFallback bool

		index map[uint32]endpoint.Endpoint

		prefer   []endpoint.Endpoint
		fallback []endpoint.Endpoint
		all      []endpoint.Endpoint

		rand xrand.Rand
	}
	option func(s *state)
)

func WithFilter(filter func(e endpoint.Info) bool) option {
	return func(s *state) {
		s.filter = filter
	}
}

func WithFallback() option {
	return func(s *state) {
		s.allowFallback = true
	}
}

func withRand(rand xrand.Rand) option {
	return func(s *state) {
		s.rand = rand
	}
}

func New(endpoints []endpoint.Endpoint, opts ...option) *state {
	s := &state{
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

func (s *state) All() (all []endpoint.Endpoint) {
	if s == nil {
		return nil
	}

	return s.all
}

func (s *state) Exclude(e endpoint.Endpoint) *state {
	return New(xslices.Filter(s.all, func(endpoint endpoint.Endpoint) bool {
		return e.Address() != endpoint.Address()
	}), withRand(s.rand))
}

func (s *state) Next(ctx context.Context) (endpoint.Endpoint, error) {
	if s == nil {
		return nil, ErrNilState
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
