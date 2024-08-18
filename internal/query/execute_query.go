package query

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
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

func execute(
	ctx context.Context, sessionID string, c Ydb_Query_V1.QueryServiceClient,
	q string, settings executeSettings, opts ...resultOption,
) (
	_ tx.Identifier, _ *result, finalErr error,
) {
	a := allocator.New()
	defer a.Free()

	request, callOptions := executeQueryRequest(a, sessionID, q, settings)

	executeCtx := xcontext.ValueOnly(ctx)

	stream, err := c.ExecuteQuery(executeCtx, request, callOptions...)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	r, txID, err := newResult(ctx, stream, append(opts, withStatsCallback(settings.StatsCallback()))...)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	if txID == "" {
		return nil, r, nil
	}

	return tx.ID(txID), r, nil
}

func readAll(ctx context.Context, r *result) error {
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

func readResultSet(ctx context.Context, r *result) (_ *resultSet, finalErr error) {
	defer func() {
		_ = r.Close(ctx)
	}()

	rs, err := r.nextResultSet(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	_, err = r.nextResultSet(ctx)
	if err == nil {
		return nil, xerrors.WithStackTrace(errMoreThanOneResultSet)
	}
	if !xerrors.Is(err, io.EOF) {
		return nil, xerrors.WithStackTrace(err)
	}

	return rs, nil
}

func readMaterializedResultSet(ctx context.Context, r *result) (_ *materializedResultSet, finalErr error) {
	defer func() {
		_ = r.Close(ctx)
	}()

	rs, err := r.nextResultSet(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	var rows []query.Row
	for {
		row, err := rs.nextRow(ctx) //nolint:govet
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

func readRow(ctx context.Context, r *result) (_ *row, finalErr error) {
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
