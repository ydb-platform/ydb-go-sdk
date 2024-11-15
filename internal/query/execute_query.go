package query

import (
	"context"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/stats"
)

type executeSettings interface {
	ExecMode() options.ExecMode
	StatsMode() options.StatsMode
	StatsCallback() func(stats stats.QueryStats)
	TxControl() *query.TransactionControl
	Syntax() options.Syntax
	Params() *params.Parameters
	CallOptions() []grpc.CallOption
	RetryOpts() []retry.Option
	ResourcePool() string
	ResponsePartLimitSizeBytes() int64
}

type executeScriptConfig interface {
	executeSettings

	ResultsTTL() time.Duration
	OperationParams() *Ydb_Operations.OperationParams
}

func executeQueryScriptRequest(a *allocator.Allocator, q string, cfg executeScriptConfig) (
	*Ydb_Query.ExecuteScriptRequest,
	[]grpc.CallOption,
) {
	request := &Ydb_Query.ExecuteScriptRequest{
		OperationParams: &Ydb_Operations.OperationParams{
			OperationMode:    0,
			OperationTimeout: nil,
			CancelAfter:      nil,
			Labels:           nil,
			ReportCostInfo:   0,
		},
		ExecMode:      Ydb_Query.ExecMode(cfg.ExecMode()),
		ScriptContent: queryQueryContent(a, Ydb_Query.Syntax(cfg.Syntax()), q),
		Parameters:    cfg.Params().ToYDB(a),
		StatsMode:     Ydb_Query.StatsMode(cfg.StatsMode()),
		ResultsTtl:    durationpb.New(cfg.ResultsTTL()),
		PoolId:        cfg.ResourcePool(),
	}

	return request, cfg.CallOptions()
}

func executeQueryRequest(a *allocator.Allocator, sessionID, q string, cfg executeSettings) (
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
	request.PoolId = cfg.ResourcePool()
	request.ResponsePartLimitBytes = cfg.ResponsePartLimitSizeBytes()

	return request, cfg.CallOptions()
}

func queryQueryContent(a *allocator.Allocator, syntax Ydb_Query.Syntax, q string) *Ydb_Query.QueryContent {
	content := a.QueryQueryContent()
	content.Syntax = syntax
	content.Text = q

	return content
}

func queryFromText(
	a *allocator.Allocator, q string, syntax Ydb_Query.Syntax,
) *Ydb_Query.ExecuteQueryRequest_QueryContent {
	content := a.QueryExecuteQueryRequestQueryContent()
	content.QueryContent = queryQueryContent(a, syntax, q)

	return content
}

func execute(
	ctx context.Context, sessionID string, c Ydb_Query_V1.QueryServiceClient,
	q string, settings executeSettings, opts ...resultOption,
) (
	_ *streamResult, finalErr error,
) {
	a := allocator.New()
	defer a.Free()

	request, callOptions := executeQueryRequest(a, sessionID, q, settings)

	executeCtx := xcontext.ValueOnly(ctx)

	stream, err := c.ExecuteQuery(executeCtx, request, callOptions...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	r, err := newResult(ctx, stream, append(opts, withStatsCallback(settings.StatsCallback()))...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func readAll(ctx context.Context, r *streamResult) error {
	defer func() {
		_ = r.Close(ctx)
	}()

	for {
		_, err := r.nextResultSet(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				return nil
			}

			return xerrors.WithStackTrace(err)
		}
	}
}

func readResultSet(ctx context.Context, r *streamResult) (_ *resultSetWithClose, finalErr error) {
	rs, err := r.nextResultSet(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	rs.mustBeLastResultSet = true

	return &resultSetWithClose{
		resultSet: rs,
		close:     r.Close,
	}, nil
}

func readMaterializedResultSet(ctx context.Context, r *streamResult) (_ *materializedResultSet, finalErr error) {
	defer func() {
		_ = r.Close(ctx)
	}()

	rs, err := r.nextResultSet(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	var rows []query.Row
	for {
		row, err := rs.nextRow(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				break
			}

			return nil, xerrors.WithStackTrace(err)
		}

		rows = append(rows, row)
	}

	_, err = r.nextResultSet(ctx)
	if err == nil {
		return nil, xerrors.WithStackTrace(errMoreThanOneResultSet)
	}
	if !xerrors.Is(err, io.EOF) {
		return nil, xerrors.WithStackTrace(err)
	}

	return MaterializedResultSet(rs.Index(), rs.Columns(), rs.ColumnTypes(), rows), nil
}

func readRow(ctx context.Context, r *streamResult) (_ *Row, finalErr error) {
	defer func() {
		_ = r.Close(ctx)
	}()

	rs, err := r.nextResultSet(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	row, err := rs.nextRow(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	_, err = rs.nextRow(ctx)
	if err == nil {
		return nil, xerrors.WithStackTrace(errMoreThanOneRow)
	}
	if !xerrors.Is(err, io.EOF) {
		return nil, xerrors.WithStackTrace(err)
	}

	_, err = r.NextResultSet(ctx)
	if err == nil {
		return nil, xerrors.WithStackTrace(errMoreThanOneResultSet)
	}
	if !xerrors.Is(err, io.EOF) {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}
