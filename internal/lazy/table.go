package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type lazyTable struct {
	db     database.Connection
	config config.Config
	c      table.Client
	m      sync.Mutex
}

func Table(db database.Connection, options []config.Option) table.Client {
	return &lazyTable{
		db:     db,
		config: config.New(options...),
	}
}

func (t *lazyTable) CreateSession(ctx context.Context, opts ...table.Option) (s table.ClosableSession, err error) {
	return t.client().CreateSession(ctx, opts...)
}

func (t *lazyTable) Do(ctx context.Context, op table.Operation, opts ...table.Option) (err error) {
	return t.client().Do(ctx, op, opts...)
}

func (t *lazyTable) DoTx(ctx context.Context, op table.TxOperation, opts ...table.Option) (err error) {
	return t.client().DoTx(ctx, op, opts...)
}

func (t *lazyTable) Close(ctx context.Context) (err error) {
	t.m.Lock()
	defer t.m.Unlock()
	if t.c == nil {
		return nil
	}
	return t.c.Close(ctx)
}

func (t *lazyTable) client() table.Client {
	t.m.Lock()
	defer t.m.Unlock()
	if t.c == nil {
		t.c = builder.New(t.db, t.config)
	}
	return t.c
}
