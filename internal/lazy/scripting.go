package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/database"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type lazyScripting struct {
	db      database.Connection
	options []config.Option
	c       scripting.Client
	m       sync.Mutex
}

func (s *lazyScripting) Execute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (res result.Result, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err = s.client().Execute(ctx, query, params)
		return xerrors.WithStackTrace(err)
	})
	return res, xerrors.WithStackTrace(err)
}

func (s *lazyScripting) Explain(
	ctx context.Context,
	query string,
	mode scripting.ExplainMode,
) (e table.ScriptingYQLExplanation, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		e, err = s.client().Explain(ctx, query, mode)
		return xerrors.WithStackTrace(err)
	})
	return e, xerrors.WithStackTrace(err)
}

func (s *lazyScripting) StreamExecute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (res result.StreamResult, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err = s.client().StreamExecute(ctx, query, params)
		return xerrors.WithStackTrace(err)
	})
	return res, xerrors.WithStackTrace(err)
}

func (s *lazyScripting) Close(ctx context.Context) (err error) {
	s.m.Lock()
	defer s.m.Unlock()
	if s.c == nil {
		return nil
	}
	defer func() {
		s.c = nil
	}()
	err = s.c.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func Scripting(db database.Connection, options []config.Option) scripting.Client {
	return &lazyScripting{
		db:      db,
		options: options,
	}
}

func (s *lazyScripting) client() scripting.Client {
	s.m.Lock()
	defer s.m.Unlock()
	if s.c == nil {
		s.c = builder.New(s.db, s.options)
	}
	return s.c
}
