package ydb

import (
	"context"
	"sync"
	"time"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type lazyTable struct {
	db      DB
	options []config.Option
	client  table.Client
	m       sync.Mutex
}

func (t *lazyTable) Pool(ctx context.Context) internal.Client {
	t.init(ctx)
	return t.client.(internal.Client)
}

func (t *lazyTable) Close(ctx context.Context) error {
	t.m.Lock()
	defer t.m.Unlock()
	if t.client == nil {
		return nil
	}
	defer func() {
		t.client = nil
	}()
	return t.client.Close(ctx)
}

func (t *lazyTable) RetryIdempotent(ctx context.Context, op table.RetryOperation) (err error) {
	t.init(ctx)
	return t.client.RetryIdempotent(ctx, op)
}

func (t *lazyTable) RetryNonIdempotent(ctx context.Context, op table.RetryOperation) (err error) {
	t.init(ctx)
	return t.client.RetryNonIdempotent(ctx, op)

}

func (t *lazyTable) init(ctx context.Context) {
	t.m.Lock()
	if t.client == nil {
		t.client = internal.New(ctx, t.db, t.options...)
	}
	t.m.Unlock()
}

func WithTableConfigOption(option config.Option) Option {
	return func(ctx context.Context, c *db) error {
		c.table.options = append(c.table.options, option)
		return nil
	}
}

func WithSessionPoolSizeLimit(sizeLimit int) Option {
	return func(ctx context.Context, c *db) error {
		c.table.options = append(c.table.options, config.WithSizeLimit(sizeLimit))
		return nil
	}
}

func WithSessionPoolKeepAliveMinSize(keepAliveMinSize int) Option {
	return func(ctx context.Context, c *db) error {
		c.table.options = append(c.table.options, config.WithKeepAliveMinSize(keepAliveMinSize))
		return nil
	}
}

func WithSessionPoolIdleThreshold(idleThreshold time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.table.options = append(c.table.options, config.WithIdleThreshold(idleThreshold))
		return nil
	}
}

func WithSessionPoolKeepAliveTimeout(keepAliveTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.table.options = append(c.table.options, config.WithKeepAliveTimeout(keepAliveTimeout))
		return nil
	}
}

func WithSessionPoolCreateSessionTimeout(createSessionTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.table.options = append(c.table.options, config.WithCreateSessionTimeout(createSessionTimeout))
		return nil
	}
}

func WithSessionPoolDeleteTimeout(deleteTimeout time.Duration) Option {
	return func(ctx context.Context, c *db) error {
		c.table.options = append(c.table.options, config.WithDeleteTimeout(deleteTimeout))
		return nil
	}
}

// WithTraceTable returns deadline which has associated Driver with it.
func WithTraceTable(trace trace.Table) Option {
	return func(ctx context.Context, c *db) error {
		c.table.options = append(c.table.options, config.WithTrace(trace))
		return nil
	}
}
