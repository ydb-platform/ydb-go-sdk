package ydb

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var nextID = uint64(0)

func (c *Driver) with(ctx context.Context, opts ...Option) (*Driver, uint64, error) {
	id := atomic.AddUint64(&nextID, 1)

	child, err := newConnectionFromOptions(
		ctx,
		append(
			append(
				c.opts,
				WithBalancer(
					c.config.Balancer(),
				),
				withOnClose(func(child *Driver) {
					c.childrenMtx.Lock()
					defer c.childrenMtx.Unlock()

					delete(c.children, id)
				}),
				withConnPool(c.pool),
			),
			opts...,
		)...,
	)
	if err != nil {
		return nil, 0, xerrors.WithStackTrace(err)
	}
	return child, id, nil
}

func (c *Driver) With(ctx context.Context, opts ...Option) (*Driver, error) {
	child, id, err := c.with(ctx, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	err = connect(ctx, child)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	c.childrenMtx.Lock()
	defer c.childrenMtx.Unlock()

	c.children[id] = child

	return child, nil
}
