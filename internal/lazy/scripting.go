package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting"
)

type lazyScripting struct {
	db     db.Connection
	client ydb_scripting.Client
	m      sync.Mutex
}

func (s *lazyScripting) Execute(
	ctx context.Context,
	query string,
	params *ydb_table.QueryParameters,
) (ydb_table_result.Result, error) {
	s.init()
	return s.client.Execute(ctx, query, params)
}

func (s *lazyScripting) Explain(
	ctx context.Context,
	query string,
	mode ydb_scripting.ExplainMode,
) (ydb_table.ScriptingYQLExplanation, error) {
	s.init()
	return s.client.Explain(ctx, query, mode)
}

func (s *lazyScripting) StreamExecute(
	ctx context.Context,
	query string,
	params *ydb_table.QueryParameters,
) (ydb_table_result.StreamResult, error) {
	s.init()
	return s.client.StreamExecute(ctx, query, params)
}

func (s *lazyScripting) Close(ctx context.Context) error {
	s.m.Lock()
	defer s.m.Unlock()
	if s.client == nil {
		return nil
	}
	defer func() {
		s.client = nil
	}()
	return s.client.Close(ctx)
}

func Scripting(db db.Connection) ydb_scripting.Client {
	return &lazyScripting{
		db: db,
	}
}

func (s *lazyScripting) init() {
	s.m.Lock()
	if s.client == nil {
		s.client = scripting.New(s.db)
	}
	s.m.Unlock()
}
