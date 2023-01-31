package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type statement struct {
	session *session
	query   query
	params  map[string]*Ydb.Type
}

// Execute executes prepared data query.
func (s *statement) Execute(
	ctx context.Context, txControl *table.TransactionControl,
	params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (
	txr table.Transaction, r result.Result, err error,
) {
	var (
		a       = allocator.New()
		request = a.TableExecuteDataQueryRequest()
	)
	defer a.Free()

	request.SessionId = s.session.id
	request.TxControl = txControl.Desc()
	request.Parameters = params.Params().ToYDB(a)
	request.Query = s.query.toYDB(a)
	request.QueryCachePolicy = a.TableQueryCachePolicy()
	request.QueryCachePolicy.KeepInCache = len(params.Params()) > 0
	request.OperationParams = operation.Params(ctx,
		s.session.config.OperationTimeout(),
		s.session.config.OperationCancelAfter(),
		operation.ModeSync,
	)

	for _, opt := range opts {
		if opt != nil {
			opt((*options.ExecuteDataQueryDesc)(request), a)
		}
	}

	onDone := trace.TableOnSessionQueryExecute(
		s.session.config.Trace(),
		&ctx,
		s.session,
		s.query,
		params,
		request.QueryCachePolicy.GetKeepInCache(),
	)
	defer func() {
		onDone(txr, true, r, err)
	}()

	return s.execute(ctx, a, request, request.TxControl)
}

// execute executes prepared query without any tracing.
func (s *statement) execute(
	ctx context.Context, a *allocator.Allocator,
	request *Ydb_Table.ExecuteDataQueryRequest, txControl *Ydb_Table.TransactionControl,
) (
	txr table.Transaction, r result.Result, err error,
) {
	res, err := s.session.executeDataQuery(ctx, a, request)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}
	return s.session.executeQueryResult(res, txControl)
}

func (s *statement) NumInput() int {
	return len(s.params)
}

func (s *statement) Text() string {
	return s.query.YQL()
}
