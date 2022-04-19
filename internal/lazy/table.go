package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type lazyTable struct {
	db      database.Connection
	options []config.Option
	c       table.Client
	m       sync.Mutex
}

func Table(db database.Connection, options []config.Option) table.Client {
	return &lazyTable{
		db:      db,
		options: options,
	}
}

func (t *lazyTable) CreateSession(ctx context.Context, opts ...table.Option) (s table.ClosableSession, err error) {
	return t.client(ctx).CreateSession(ctx, opts...)
}

func (t *lazyTable) Do(ctx context.Context, op table.Operation, opts ...table.Option) (err error) {
	return t.client(ctx).Do(ctx, op, opts...)
}

func (t *lazyTable) DoTx(ctx context.Context, op table.TxOperation, opts ...table.Option) (err error) {
	return t.client(ctx).DoTx(ctx, op, opts...)
}

func (t *lazyTable) Close(ctx context.Context) (err error) {
	t.m.Lock()
	defer t.m.Unlock()
	if t.c == nil {
		return nil
	}
	defer func() {
		t.c = nil
	}()
	err = t.c.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func (t *lazyTable) client(ctx context.Context) table.Client {
	t.m.Lock()
	defer t.m.Unlock()
	if t.c == nil {
		t.c = builder.New(ctx, t.db, t.options...)
	}
	return t.c
}
