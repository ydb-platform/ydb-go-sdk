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

func (t *lazyTable) Do(ctx context.Context, op table.Operation, opts ...table.Option) (err error) {
	t.init(ctx)
	return t.client.Do(ctx, op, opts...)
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
