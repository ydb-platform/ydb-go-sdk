package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type lazyScripting struct {
	db      db.Connection
	options []config.Option
	client  scripting.Client
	m       sync.Mutex
}

func (s *lazyScripting) Execute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (res result.Result, err error) {
	s.init()
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err = s.client.Execute(ctx, query, params)
		return err
	})
	return res, err
}

func (s *lazyScripting) Explain(
	ctx context.Context,
	query string,
	mode scripting.ExplainMode,
) (e table.ScriptingYQLExplanation, err error) {
	s.init()
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		e, err = s.client.Explain(ctx, query, mode)
		return err
	})
	return e, err
}

func (s *lazyScripting) StreamExecute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (res result.StreamResult, err error) {
	s.init()
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err = s.client.StreamExecute(ctx, query, params)
		return err
	})
	return res, err
}

func (s *lazyScripting) Close(ctx context.Context) (err error) {
	s.m.Lock()
	defer s.m.Unlock()
	if s.client == nil {
		return nil
	}
	defer func() {
		s.client = nil
	}()
	err = s.client.Close(ctx)
	if err != nil {
		return errors.WithStackTrace(err)
	}
	return nil
}

func Scripting(db db.Connection, options []config.Option) scripting.Client {
	return &lazyScripting{
		db:      db,
		options: options,
	}
}

func (s *lazyScripting) init() {
	s.m.Lock()
	if s.client == nil {
		s.client = builder.New(s.db, s.options)
	}
	s.m.Unlock()
}
