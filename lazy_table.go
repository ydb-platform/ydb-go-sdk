package ydb

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type lazyTable struct {
	db     DB
	config internal.Config
	client table.Client
	m      sync.Mutex
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
	t.init()
	return t.client.RetryIdempotent(ctx, op)
}

func (t *lazyTable) RetryNonIdempotent(ctx context.Context, op table.RetryOperation) (err error) {
	t.init()
	return t.client.RetryNonIdempotent(ctx, op)

}

func newTable(db DB, config internal.Config) *lazyTable {
	return &lazyTable{
		db:     db,
		config: config,
	}
}

func (t *lazyTable) init() {
	t.m.Lock()
	if assert.IsNil(t.client) {
		t.client = internal.NewClientAsPool(t.db, t.config)
	}
	t.m.Unlock()
}

func tableConfig(o options) internal.Config {
	config := internal.DefaultConfig()
	if o.traceTable != nil {
		config.Trace = *o.traceTable
	}
	if o.tableSessionPoolSizeLimit != nil {
		config.SizeLimit = *o.tableSessionPoolSizeLimit
	}
	if o.tableSessionPoolKeepAliveMinSize != nil {
		config.KeepAliveMinSize = *o.tableSessionPoolKeepAliveMinSize
	}
	if o.tableSessionPoolIdleThreshold != nil {
		config.IdleThreshold = *o.tableSessionPoolIdleThreshold
	}
	if o.tableSessionPoolKeepAliveTimeout != nil {
		config.KeepAliveTimeout = *o.tableSessionPoolKeepAliveTimeout
	}
	if o.tableSessionPoolCreateSessionTimeout != nil {
		config.CreateSessionTimeout = *o.tableSessionPoolCreateSessionTimeout
	}
	if o.tableSessionPoolDeleteTimeout != nil {
		config.DeleteTimeout = *o.tableSessionPoolDeleteTimeout
	}
	return config
}

func (t *lazyTable) CreateSession(ctx context.Context) (table.Session, error) {
	t.init()
	return t.client.CreateSession(ctx)
}
