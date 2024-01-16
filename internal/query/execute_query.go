package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type executeSettings interface {
	ExecMode() query.ExecMode
	StatsMode() query.StatsMode
	TxControl() *query.TransactionControl
	Syntax() query.Syntax
	Params() *query.Parameters
	CallOptions() []grpc.CallOption
}

func executeQueryRequest(a *allocator.Allocator, sessionID string, q string, settings executeSettings) (
	*Ydb_Query.ExecuteQueryRequest,
	[]grpc.CallOption,
) {
	request := a.QueryExecuteQueryRequest()

	request.SessionId = sessionID
	request.ExecMode = Ydb_Query.ExecMode(settings.ExecMode())
	request.TxControl = settings.TxControl().ToYDB(a)
	request.Query = queryFromText(a, q, settings.Syntax())
	request.Parameters = settings.Params().ToYDB(a)
	request.StatsMode = Ydb_Query.StatsMode(settings.StatsMode())
	request.ConcurrentResultSets = false

	return request, settings.CallOptions()
}

func queryFromText(a *allocator.Allocator, q string, syntax Ydb_Query.Syntax) *Ydb_Query.ExecuteQueryRequest_QueryContent {
	content := a.QueryExecuteQueryRequestQueryContent()
	content.QueryContent = a.QueryQueryContent()
	content.QueryContent.Syntax = syntax
	content.QueryContent.Text = q
	return content
}

func execute(
	ctx context.Context, session *Session, client Ydb_Query_V1.QueryServiceClient, q string, settings executeSettings,
) (*transaction, *result, error) {
	a := allocator.New()
	request, callOptions := executeQueryRequest(a, session.id, q, settings)
	defer func() {
		a.Free()
	}()

	ctx, cancel := xcontext.WithCancel(ctx)

	stream, err := client.ExecuteQuery(ctx, request, callOptions...)
	if err != nil {
		cancel()
		return nil, nil, xerrors.WithStackTrace(err)
	}
	r, txID, err := newResult(ctx, stream, cancel)
	if err != nil {
		cancel()
		return nil, nil, xerrors.WithStackTrace(err)
	}
	return &transaction{
		id: txID,
		s:  session,
	}, r, nil
}
