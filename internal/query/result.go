package query

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ query.Result = (*result)(nil)
	_ query.Result = (*materializedResult)(nil)
)

type (
	materializedResult struct {
		resultSets []query.ResultSet
		idx        int
		stats      stats.QueryStats
	}
	result struct {
		stream         Ydb_Query_V1.QueryService_ExecuteQueryClient
		closeOnce      func(ctx context.Context) error
		lastPart       *Ydb_Query.ExecuteQueryResponsePart
		stats          *Ydb_TableStats.QueryStats
		resultSetIndex int64
		errs           []error
		closed         chan struct{}
		trace          *trace.Query
	}
)

func (r *materializedResult) Stats() stats.QueryStats {
	return r.stats
}

func (r *materializedResult) Range(ctx context.Context) xiter.Seq2[query.ResultSet, error] {
	return rangeResultSets(ctx, r)
}

func (r *result) Range(ctx context.Context) xiter.Seq2[query.ResultSet, error] {
	return rangeResultSets(ctx, r)
}

func (r *materializedResult) Close(ctx context.Context) error {
	return nil
}

func (r *materializedResult) NextResultSet(ctx context.Context) (query.ResultSet, error) {
	if r.idx == len(r.resultSets) {
		return nil, xerrors.WithStackTrace(io.EOF)
	}

	defer func() {
		r.idx++
	}()

	return r.resultSets[r.idx], nil
}

func (r *materializedResult) Err() error {
	return nil
}

func newMaterializedResult(resultSets []query.ResultSet, stats stats.QueryStats) *materializedResult {
	return &materializedResult{
		resultSets: resultSets,
		stats:      stats,
	}
}

func newResult(
	ctx context.Context,
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	t *trace.Query,
	closeResult context.CancelFunc,
) (_ *result, txID string, err error) {
	if t == nil {
		t = &trace.Query{}
	}
	if closeResult == nil {
		closeResult = func() {}
	}

	onDone := trace.QueryOnResultNew(t, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.newResult"),
	)
	defer func() {
		onDone(err)
	}()

	select {
	case <-ctx.Done():
		return nil, txID, xerrors.WithStackTrace(ctx.Err())
	default:
		part, err := nextPart(ctx, stream, t)
		if err != nil {
			return nil, txID, xerrors.WithStackTrace(err)
		}
		var (
			closed    = make(chan struct{})
			closeOnce = xsync.OnceFunc(func(ctx context.Context) error {
				closeResult()

				close(closed)

				return nil
			})
		)

		return &result{
			stream:         stream,
			resultSetIndex: -1,
			lastPart:       part,
			stats:          part.GetExecStats(),
			closed:         closed,
			closeOnce:      closeOnce,
			trace:          t,
		}, part.GetTxMeta().GetId(), nil
	}
}

func (r *result) Stats() stats.QueryStats {
	return stats.FromQueryStats(r.stats)
}

func nextPart(
	ctx context.Context,
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	t *trace.Query,
) (part *Ydb_Query.ExecuteQueryResponsePart, err error) {
	if t == nil {
		t = &trace.Query{}
	}

	onDone := trace.QueryOnResultNextPart(t, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.nextPart"),
	)
	defer func() {
		onDone(part.GetExecStats(), err)
	}()

	part, err = stream.Recv()
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return part, nil
}

func (r *result) Close(ctx context.Context) (err error) {
	onDone := trace.QueryOnResultClose(r.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*result).Close"),
	)
	defer func() {
		onDone(err)
	}()

	return r.closeOnce(ctx)
}

func (r *result) nextResultSet(ctx context.Context) (_ *resultSet, err error) {
	defer func() {
		if err != nil && !xerrors.Is(err,
			io.EOF, errClosedResult, context.Canceled,
		) {
			r.errs = append(r.errs, err)
		}
	}()
	nextResultSetIndex := r.resultSetIndex + 1
	for {
		select {
		case <-r.closed:
			return nil, xerrors.WithStackTrace(errClosedResult)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		default:
			if resultSetIndex := r.lastPart.GetResultSetIndex(); resultSetIndex >= nextResultSetIndex {
				r.resultSetIndex = resultSetIndex

				return newResultSet(r.getNextResultSetPart(ctx, nextResultSetIndex), r.lastPart, r.trace), nil
			}
			if r.stream == nil {
				return nil, xerrors.WithStackTrace(io.EOF)
			}
			part, err := nextPart(ctx, r.stream, r.trace)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					r.stream = nil
				}

				return nil, xerrors.WithStackTrace(err)
			}
			if stats := part.GetExecStats(); stats != nil {
				r.stats = stats
			}
			if part.GetResultSetIndex() < r.resultSetIndex {
				return nil, xerrors.WithStackTrace(fmt.Errorf(
					"next result set rowIndex %d less than last result set index %d: %w",
					part.GetResultSetIndex(), r.resultSetIndex, errWrongNextResultSetIndex,
				))
			}
			r.lastPart = part
			r.resultSetIndex = part.GetResultSetIndex()
		}
	}
}

func (r *result) getNextResultSetPart(
	ctx context.Context,
	nextResultSetIndex int64,
) func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
	return func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
		defer func() {
			if err != nil && !xerrors.Is(err,
				io.EOF, context.Canceled,
			) {
				r.errs = append(r.errs, err)
			}
		}()
		select {
		case <-r.closed:
			return nil, xerrors.WithStackTrace(errClosedResult)
		default:
			if r.stream == nil {
				return nil, xerrors.WithStackTrace(io.EOF)
			}
			part, err := nextPart(ctx, r.stream, r.trace)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					r.stream = nil
				}

				return nil, xerrors.WithStackTrace(err)
			}
			r.lastPart = part
			if stats := part.GetExecStats(); stats != nil {
				r.stats = stats
			}
			if part.GetResultSetIndex() > nextResultSetIndex {
				return nil, xerrors.WithStackTrace(fmt.Errorf(
					"result set (index=%d) receive part (index=%d) for next result set: %w",
					nextResultSetIndex, part.GetResultSetIndex(), io.EOF,
				))
			}

			return part, nil
		}
	}
}

func (r *result) NextResultSet(ctx context.Context) (_ query.ResultSet, err error) {
	ctx, cancel := xcontext.WithDone(ctx, r.closed)
	defer cancel()

	onDone := trace.QueryOnResultNextResultSet(r.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*result).NextResultSet"),
	)
	defer func() {
		onDone(err)
	}()

	return r.nextResultSet(ctx)
}

func (r *result) Err() error {
	switch {
	case len(r.errs) == 0:
		return nil
	case len(r.errs) == 1:
		return r.errs[0]
	default:
		return xerrors.WithStackTrace(xerrors.Join(r.errs...))
	}
}

func exactlyOneRowFromResult(ctx context.Context, r query.Result) (row query.Row, err error) {
	rs, err := r.NextResultSet(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	row, err = rs.NextRow(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	_, err = rs.NextRow(ctx)
	switch {
	case err == nil:
		return nil, xerrors.WithStackTrace(errMoreThanOneRow)
	case errors.Is(err, io.EOF):
		// pass
	default:
		return nil, xerrors.WithStackTrace(err)
	}

	_, err = r.NextResultSet(ctx)
	switch {
	case err == nil:
		return nil, xerrors.WithStackTrace(errMoreThanOneRow)
	case errors.Is(err, io.EOF):
		// pass
	default:
		return nil, xerrors.WithStackTrace(err)
	}

	if err = r.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func exactlyOneResultSetFromResult(ctx context.Context, r query.Result) (rs query.ResultSet, err error) {
	var rows []query.Row
	rs, err = r.NextResultSet(ctx)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return nil, xerrors.WithStackTrace(errNoResultSets)
		}

		return nil, xerrors.WithStackTrace(err)
	}

	var row query.Row
	for {
		row, err = rs.NextRow(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				break
			}

			return nil, xerrors.WithStackTrace(err)
		}

		rows = append(rows, row)
	}

	_, err = r.NextResultSet(ctx)
	switch {
	case err == nil:
		return nil, xerrors.WithStackTrace(errMoreThanOneResultSet)
	case errors.Is(err, io.EOF):
		// pass
	default:
		return nil, xerrors.WithStackTrace(err)
	}

	if err = r.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return NewMaterializedResultSet(rs.Index(), rs.Columns(), rs.ColumnTypes(), rows), nil
}

func resultToMaterializedResult(ctx context.Context, r query.Result) (query.Result, error) {
	var resultSets []query.ResultSet

	for {
		rs, err := r.NextResultSet(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				break
			}

			return nil, xerrors.WithStackTrace(err)
		}

		var rows []query.Row
		for {
			row, err := rs.NextRow(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					break
				}

				return nil, xerrors.WithStackTrace(err)
			}

			rows = append(rows, row)
		}

		resultSets = append(resultSets, NewMaterializedResultSet(rs.Index(), rs.Columns(), rs.ColumnTypes(), rows))
	}

	return newMaterializedResult(resultSets, r.Stats()), nil
}
