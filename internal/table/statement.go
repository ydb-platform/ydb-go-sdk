package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type statement struct {
	session *Session
	query   Query
	params  map[string]*Ydb.Type
}

// Execute executes prepared data query.
func (s *statement) Execute(
	ctx context.Context, txControl *table.TransactionControl,
	parameters *params.Params,
	opts ...options.ExecuteDataQueryOption,
) (txr table.Transaction, r result.Result, err error) {
	ydbParams, _ := parameters.ToYDB()
	queryCachePolicy := Ydb_Table.QueryCachePolicy_builder{
		KeepInCache: len(ydbParams) > 0,
	}.Build()
	operationParams := operation.Params(ctx,
		s.session.config.OperationTimeout(),
		s.session.config.OperationCancelAfter(),
		operation.ModeSync,
	)

	var (
		request = options.ExecuteDataQueryDesc{
			ExecuteDataQueryRequest: Ydb_Table.ExecuteDataQueryRequest_builder{
				SessionId:        s.session.id,
				TxControl:        txControl.ToYdbTableTransactionControl(),
				Parameters:       ydbParams,
				Query:            s.query.toYDB(),
				QueryCachePolicy: queryCachePolicy,
				OperationParams:  operationParams,
			}.Build(),
			IgnoreTruncated: s.session.config.IgnoreTruncated(),
		}
		callOptions []grpc.CallOption
	)

	for _, opt := range opts {
		if opt != nil {
			callOptions = append(callOptions, opt.ApplyExecuteDataQueryOption(&request)...)
		}
	}

	onDone := gtrace.TableOnSessionQueryExecute(
		s.session.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table.(*statement).Execute"),
		s.session, s.query, parameters,
		queryCachePolicy.GetKeepInCache(),
	)
	defer func() {
		onDone(txr, true, r, err)
	}()

	return s.execute(ctx, txControl, &request, callOptions...)
}

// execute executes prepared query without any tracing.
func (s *statement) execute(
	ctx context.Context,
	txControl *tx.Control,
	request *options.ExecuteDataQueryDesc,
	callOptions ...grpc.CallOption,
) (
	txr table.Transaction, r result.Result, err error,
) {
	t, r, err := s.session.dataQuery.execute(ctx, txControl, request.ExecuteDataQueryRequest, callOptions...)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	if t != nil {
		t.s = s.session
	}

	return t, r, nil
}

func (s *statement) NumInput() int {
	return len(s.params)
}

func (s *statement) Text() string {
	return s.query.YQL()
}
