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

func From(parent *Cluster) option {
	if parent == nil {
		return nil
	}

	return func(s *Cluster) {
		s.rand = parent.rand
		s.filter = parent.filter
		s.allowFallback = parent.allowFallback
	}
}

func New(endpoints []endpoint.Endpoint, opts ...option) *Cluster {
	c := &Cluster{
		filter: func(e endpoint.Info) bool {
			return true
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	if c.rand == nil {
		c.rand = xrand.New(xrand.WithLock())
	}

	c.prefer, c.fallback = xslices.Split(endpoints, func(e endpoint.Endpoint) bool {
		return c.filter(e)
	})

	if c.allowFallback {
		c.all = endpoints
		c.index = xslices.Map(endpoints, func(e endpoint.Endpoint) uint32 { return e.NodeID() })
	} else {
		c.all = c.prefer
		c.fallback = nil
		c.index = xslices.Map(c.prefer, func(e endpoint.Endpoint) uint32 { return e.NodeID() })
	}

	return c
}

func (c *Cluster) All() (all []endpoint.Endpoint) {
	if c == nil {
		return nil
	}

	return c.all
}

func (c *Cluster) Availability() (percent float64) {
	return float64(len(c.prefer)+len(c.fallback)) / float64(len(c.all))
}

func Without(s *Cluster, required endpoint.Endpoint, other ...endpoint.Endpoint) *Cluster {
	var (
		prefer   = make([]endpoint.Endpoint, 0, len(s.prefer))
		fallback = make([]endpoint.Endpoint, 0, len(s.fallback))
	)

	for _, endpoint := range append(other, required) {
		for i := range s.prefer {
			if s.prefer[i].Address() != endpoint.Address() {
				prefer = append(prefer, s.prefer[i])
			}
		}
		for i := range s.fallback {
			if s.fallback[i].Address() != endpoint.Address() {
				fallback = append(fallback, s.fallback[i])
			}
		}
	}

	if len(prefer)+len(fallback) == len(s.prefer)+len(s.fallback) {
		return s
	}

	all := append(append(make([]endpoint.Endpoint, 0, len(prefer)+len(fallback)), prefer...), fallback...)

	return &Cluster{
		filter:        s.filter,
		allowFallback: s.allowFallback,
		index:         xslices.Map(all, func(e endpoint.Endpoint) uint32 { return e.NodeID() }),
		prefer:        prefer,
		fallback:      fallback,
		all:           s.all,
		rand:          s.rand,
	}
}

func (c *Cluster) Next(ctx context.Context) (endpoint.Endpoint, error) {
	if c == nil {
		return nil, ErrNilPtr
	}

	if err := ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if nodeID, wantEndpointByNodeID := endpoint.ContextNodeID(ctx); wantEndpointByNodeID {
		e, has := c.index[nodeID]
		if has {
			return e, nil
		}
	}

	if l := len(c.prefer); l > 0 {
		return c.prefer[c.rand.Int(l)], nil
	}

	if l := len(c.fallback); l > 0 {
		return c.fallback[c.rand.Int(l)], nil
	}

	return nil, xerrors.WithStackTrace(ErrNoEndpoints)
}
