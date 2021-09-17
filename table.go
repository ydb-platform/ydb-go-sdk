package ydb

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type lazyTable struct {
	db     DB
	config table.Config
	client table.Client
	once   sync.Once
}

func (t *lazyTable) Close(ctx context.Context) error {
	t.init()
	return t.client.Close(ctx)
}

func (t *lazyTable) Do(ctx context.Context, retryNoIdempotent bool, op table.RetryOperation) (err error) {
	t.init()
	return t.client.Do(ctx, retryNoIdempotent, op)
}

func newTable(db DB, config table.Config) *lazyTable {
	return &lazyTable{
		db:     db,
		config: config,
	}
}

func (t *lazyTable) init() {
	t.once.Do(func() {
		t.client = table.NewClient(t.db, t.config)
	})
}

func tableConfig(o options) table.Config {
	config := table.Config{}
	if o.tableClientTrace != nil {
		config.Trace = *o.tableClientTrace
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

func (t *lazyTable) CreateSession(ctx context.Context) (*table.Session, error) {
	t.init()
	return t.client.CreateSession(ctx)
}
