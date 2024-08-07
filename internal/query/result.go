package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
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
		closeOnce      func()
		lastPart       *Ydb_Query.ExecuteQueryResponsePart
		stats          *Ydb_TableStats.QueryStats
		resultSetIndex int64
		closed         chan struct{}
		trace          *trace.Query
		onNextPartErr  []func(err error)
	}
)

func (r *materializedResult) Stats() stats.QueryStats {
	return r.stats
}

func (r *materializedResult) ResultSets(ctx context.Context) xiter.Seq2[query.ResultSet, error] {
	return rangeResultSets(ctx, r)
}

func (r *result) ResultSets(ctx context.Context) xiter.Seq2[query.ResultSet, error] {
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

func newMaterializedResult(resultSets []query.ResultSet, stats stats.QueryStats) *materializedResult {
	return &materializedResult{
		resultSets: resultSets,
		stats:      stats,
	}
}

type (
	resultOption func(s *result)
)

func withTrace(t *trace.Query) resultOption {
	return func(s *result) {
		s.trace = t
	}
}

func onNextPartErr(callback func(err error)) resultOption {
	return func(s *result) {
		s.onNextPartErr = append(s.onNextPartErr, callback)
	}
}

func newResult(
	ctx context.Context,
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	opts ...resultOption,
) (_ *result, txID string, finalErr error) {
	r := result{
		stream:         stream,
		closed:         make(chan struct{}),
		resultSetIndex: -1,
	}
	r.closeOnce = sync.OnceFunc(func() {
		close(r.closed)
		r.stream = nil
	})

	for _, opt := range opts {
		if opt != nil {
			opt(&r)
		}
	}

	if r.trace != nil {
		onDone := trace.QueryOnResultNew(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.newResult"),
		)
		defer func() {
			onDone(finalErr)
		}()
	}

	select {
	case <-ctx.Done():
		return nil, txID, xerrors.WithStackTrace(ctx.Err())
	default:
		part, err := r.nextPart(ctx)
		if err != nil {
			return nil, txID, xerrors.WithStackTrace(err)
		}

		r.lastPart = part
		r.stats = part.GetExecStats()

		return &r, part.GetTxMeta().GetId(), nil
	}
}

func (r *result) Stats() stats.QueryStats {
	return stats.FromQueryStats(r.stats)
}

func (r *result) nextPart(ctx context.Context) (
	part *Ydb_Query.ExecuteQueryResponsePart, err error,
) {
	if r.trace != nil {
		onDone := trace.QueryOnResultNextPart(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*result).nextPart"),
		)
		defer func() {
			onDone(part.GetExecStats(), err)
		}()
	}

	select {
	case <-r.closed:
		return nil, xerrors.WithStackTrace(io.EOF)
	default:
		part, err = nextPart(r.stream)
		if err != nil {
			r.closeOnce()

			for _, callback := range r.onNextPartErr {
				callback(err)
			}

			return nil, xerrors.WithStackTrace(err)
		}

		return part, nil
	}
}

func nextPart(stream Ydb_Query_V1.QueryService_ExecuteQueryClient) (
	part *Ydb_Query.ExecuteQueryResponsePart, err error,
) {
	part, err = stream.Recv()
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return part, nil
}

func (r *result) Close(ctx context.Context) (finalErr error) {
	defer r.closeOnce()

	if r.trace != nil {
		onDone := trace.QueryOnResultClose(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*result).Close"),
		)
		defer func() {
			onDone(finalErr)
		}()
	}

	for {
		select {
		case <-r.closed:
			return nil
		default:
			_, err := r.nextPart(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					return nil
				}

				return xerrors.WithStackTrace(err)
			}
		}
	}
}

func (r *result) nextResultSet(ctx context.Context) (_ *resultSet, err error) {
	nextResultSetIndex := r.resultSetIndex + 1
	for {
		select {
		case <-r.closed:
			return nil, xerrors.WithStackTrace(io.EOF)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		default:
			if resultSetIndex := r.lastPart.GetResultSetIndex(); resultSetIndex >= nextResultSetIndex {
				r.resultSetIndex = resultSetIndex

				return newResultSet(r.nextPartFunc(ctx, nextResultSetIndex), r.lastPart, r.trace), nil
			}
			if r.stream == nil {
				return nil, xerrors.WithStackTrace(io.EOF)
			}
			part, err := r.nextPart(ctx)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			if stats := part.GetExecStats(); stats != nil {
				r.stats = stats
			}
			if part.GetResultSetIndex() < r.resultSetIndex {
				r.closeOnce()

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

func (r *result) nextPartFunc(
	ctx context.Context,
	nextResultSetIndex int64,
) func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
	return func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
		select {
		case <-r.closed:
			return nil, xerrors.WithStackTrace(io.EOF)
		default:
			if r.stream == nil {
				return nil, xerrors.WithStackTrace(io.EOF)
			}
			part, err := r.nextPart(ctx)
			if err != nil {
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
	if r.trace != nil {
		onDone := trace.QueryOnResultNextResultSet(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*result).NextResultSet"),
		)
		defer func() {
			onDone(err)
		}()
	}

	return r.nextResultSet(ctx)
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
