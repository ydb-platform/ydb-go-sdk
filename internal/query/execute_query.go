package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type executeConfig interface {
	ExecMode() options.ExecMode
	StatsMode() options.StatsMode
	TxControl() *query.TransactionControl
	Syntax() options.Syntax
	Params() *params.Parameters
	CallOptions() []grpc.CallOption
}

func executeQueryRequest(a *allocator.Allocator, sessionID, q string, cfg executeConfig) (
	*Ydb_Query.ExecuteQueryRequest,
	[]grpc.CallOption,
) {
	request := a.QueryExecuteQueryRequest()

	request.SessionId = sessionID
	request.ExecMode = Ydb_Query.ExecMode(cfg.ExecMode())
	request.TxControl = cfg.TxControl().ToYDB(a)
	request.Query = queryFromText(a, q, Ydb_Query.Syntax(cfg.Syntax()))
	request.Parameters = cfg.Params().ToYDB(a)
	request.StatsMode = Ydb_Query.StatsMode(cfg.StatsMode())
	request.ConcurrentResultSets = false

	return request, cfg.CallOptions()
}

func queryFromText(
	a *allocator.Allocator, q string, syntax Ydb_Query.Syntax,
) *Ydb_Query.ExecuteQueryRequest_QueryContent {
	content := a.QueryExecuteQueryRequestQueryContent()
	content.QueryContent = a.QueryQueryContent()
	content.QueryContent.Syntax = syntax
	content.QueryContent.Text = q

	return content
}

func execute(ctx context.Context, s *Session, c Ydb_Query_V1.QueryServiceClient, q string, cfg executeConfig) (
	_ *transaction, _ *result, finalErr error,
) {
	a := allocator.New()
	defer a.Free()

	request, callOptions := executeQueryRequest(a, s.id, q, cfg)

	executeCtx, cancelExecute := xcontext.WithCancel(xcontext.ValueOnly(ctx))

	stream, err := c.ExecuteQuery(executeCtx, request, callOptions...)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	r, txID, err := newResult(ctx, stream, s.cfg.Trace(), cancelExecute)
	if err != nil {
		cancelExecute()

		return nil, nil, xerrors.WithStackTrace(err)
	}

	if txID == "" {
		return nil, r, nil
	}

	return newTransaction(txID, s), r, nil
}
