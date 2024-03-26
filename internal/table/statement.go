package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
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
	parameters *params.Parameters,
	opts ...options.ExecuteDataQueryOption,
) (
	txr table.Transaction, r result.Result, err error,
) {
	var (
		a       = allocator.New()
		request = options.ExecuteDataQueryDesc{
			ExecuteDataQueryRequest: a.TableExecuteDataQueryRequest(),
			IgnoreTruncated:         s.session.config.IgnoreTruncated(),
		}
		callOptions []grpc.CallOption
	)
	defer a.Free()

	request.SessionId = s.session.id
	request.TxControl = txControl.Desc()
	request.Parameters = parameters.ToYDB(a)
	request.Query = s.query.toYDB(a)
	request.QueryCachePolicy = a.TableQueryCachePolicy()
	request.QueryCachePolicy.KeepInCache = len(request.Parameters) > 0
	request.OperationParams = operation.Params(ctx,
		s.session.config.OperationTimeout(),
		s.session.config.OperationCancelAfter(),
		operation.ModeSync,
	)

	for _, opt := range opts {
		if opt != nil {
			callOptions = append(callOptions, opt.ApplyExecuteDataQueryOption(&request, a)...)
		}
	}

	onDone := trace.TableOnSessionQueryExecute(
		s.session.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/table.(*statement).Execute"),
		s.session, s.query, parameters,
		request.QueryCachePolicy.GetKeepInCache(),
	)
	defer func() {
		onDone(txr, true, r, err)
	}()

	return s.execute(ctx, a, &request, request.TxControl, callOptions...)
}

// execute executes prepared query without any tracing.
func (s *statement) execute(
	ctx context.Context, a *allocator.Allocator,
	request *options.ExecuteDataQueryDesc, txControl *Ydb_Table.TransactionControl,
	callOptions ...grpc.CallOption,
) (
	txr table.Transaction, r result.Result, err error,
) {
	res, err := s.session.executeDataQuery(ctx, a, request.ExecuteDataQueryRequest, callOptions...)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	return s.session.executeQueryResult(res, txControl, request.IgnoreTruncated)
}

func (s *statement) NumInput() int {
	return len(s.params)
}

func (s *statement) Text() string {
	return s.query.YQL()
}
