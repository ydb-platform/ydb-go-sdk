package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type statement struct {
	session *session
	query   *dataQuery
	params  map[string]*Ydb.Type
}

func Params(s table.Statement) map[string]*Ydb.Type {
	return s.(*statement).params
}

// Execute executes prepared data query.
func (s *statement) Execute(
	ctx context.Context, tx *table.TransactionControl,
	params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (
	txr table.Transaction, r result.Result, err error,
) {
	var optsResult options.ExecuteDataQueryDesc
	for _, f := range opts {
		f(&optsResult)
	}

	onDone := trace.TableOnSessionQueryExecute(
		s.session.config.Trace(),
		&ctx,
		s.session,
		s.query,
		params,
		optsResult.QueryCachePolicy.GetKeepInCache(),
	)
	defer func() {
		onDone(txr, true, r, err)
	}()
	return s.execute(ctx, tx, params, opts...)
}

// execute executes prepared query without any tracing.
func (s *statement) execute(
	ctx context.Context, tx *table.TransactionControl,
	params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (
	txr table.Transaction, r result.Result, err error,
) {
	_, res, err := s.session.executeDataQuery(
		ctx,
		tx,
		s.query,
		params,
		opts...,
	)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}
	return s.session.executeQueryResult(res)
}

func (s *statement) NumInput() int {
	return len(s.params)
}

func (s *statement) Text() string {
	return s.query.YQL()
}
