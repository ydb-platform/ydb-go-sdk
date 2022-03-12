package ydb

import (
	"context"
	"sync/atomic"
)

var nextID = uint64(0)

func (c *connection) With(ctx context.Context, opts ...Option) (Connection, error) {
	if len(opts) == 0 {
		return c, nil
	}

	opts = append(
		opts,
		WithBalancer(
			c.config.Balancer().Create(),
		),
	)

	id := atomic.AddUint64(&nextID, 1)

	opts = append(
		opts,
		withConnPool(c.pool),
		withOnClose(func(child *connection) {
			c.childrenMtx.Lock()
			defer c.childrenMtx.Unlock()

			delete(c.children, id)
		}),
	)

	child, err := New(
		ctx,
		append(
			c.opts,
			opts...,
		)...,
	)
	if err != nil {
		return nil, err
	}

	c.childrenMtx.Lock()
	defer c.childrenMtx.Unlock()

	c.children[id] = child

	return child, nil
}
