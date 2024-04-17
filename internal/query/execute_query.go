package query

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type withAllocatorOption struct {
	a *allocator.Allocator
}

func (a withAllocatorOption) ApplyExecuteOption(s *options.Execute) {
	s.Allocator = a.a
}

func WithAllocator(a *allocator.Allocator) options.ExecuteOption {
	return withAllocatorOption{a: a}
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

func ReadAll(ctx context.Context, r *result) (resultSets []*Ydb.ResultSet, stats *Ydb_TableStats.QueryStats, _ error) {
	for {
		resultSet, err := r.nextResultSet(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				return resultSets, resultSet.stats(), nil
			}

			return nil, nil, xerrors.WithStackTrace(err)
		}
		var rows []*Ydb.Value
		for {
			row, err := resultSet.nextRow(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					break
				}

				return nil, nil, xerrors.WithStackTrace(err)
			}

			rows = append(rows, row.v)
		}

		resultSets = append(resultSets, &Ydb.ResultSet{
			Columns: resultSet.columns,
			Rows:    rows,
		})
	}
}

func Execute[T options.ExecuteOption](
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID, query string, opts ...T,
) (_ *transaction, _ *result, finalErr error) {
	var (
		settings = options.ExecuteSettings(opts...)
		a        = settings.Allocator
	)

	if a == nil {
		a = allocator.New()
		defer a.Free()
	}

	request := a.QueryExecuteQueryRequest()

	request.SessionId = sessionID
	request.ExecMode = Ydb_Query.ExecMode(settings.ExecMode)
	request.TxControl = settings.TxControl.ToYDB(a)
	request.Query = queryFromText(a, query, Ydb_Query.Syntax(settings.Syntax))
	request.Parameters = settings.Params.ToYDB(a)
	request.StatsMode = Ydb_Query.StatsMode(settings.StatsMode)
	request.ConcurrentResultSets = false

	executeCtx, cancelExecute := xcontext.WithCancel(xcontext.ValueOnly(ctx))

	stream, err := client.ExecuteQuery(executeCtx, request, settings.GrpcCallOptions...)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	r, txID, err := newResult(ctx, stream, settings.Trace, cancelExecute)
	if err != nil {
		cancelExecute()

		return nil, nil, xerrors.WithStackTrace(err)
	}

	if txID == "" {
		return nil, r, nil
	}

	return newTx(txID, sessionID, client, settings.Trace), r, nil
}
