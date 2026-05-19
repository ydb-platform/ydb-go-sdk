package query

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var errReadNextResultSet = xerrors.Wrap(errors.New("ydb: stop read the result set because see part of next result set"))

var (
	_ result.Result = (*streamResult)(nil)
	_ result.Result = (*materializedResult)(nil)
)

type (
	materializedResult struct {
		resultSets []result.Set
		idx        int
	}
	streamResult struct {
		stream         Ydb_Query_V1.QueryService_ExecuteQueryClient
		lastErr        error
		shutdownHooks  []func()
		lastPart       *Ydb_Query.ExecuteQueryResponsePart
		resultSetIndex int64
		trace          *trace.Query
		statsCallback  func(queryStats stats.QueryStats)
		issuesCallback func(issues []*Ydb_Issue.IssueMessage)
		onNextPartErr  []func(err error)
		onTxMeta       []func(txMeta *Ydb_Query.TransactionMeta)
		closeTimeout   time.Duration
		// streamCancel cancels the gRPC context that backs stream.Recv(). It
		// is wired up by execute() to point at the executeCtx CancelFunc and
		// invoked on demand from nextPart so that a Recv blocked on the wire
		// can be unblocked when the caller's ctx is cancelled mid-flight.
		streamCancel context.CancelFunc
		closed       atomic.Bool
	}
	resultOption func(s *streamResult)
)

func rangeResultSets(ctx context.Context, r result.Result) xiter.Seq2[result.Set, error] {
	return func(yield func(result.Set, error) bool) {
		for {
			rs, err := r.NextResultSet(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					return
				}
			}
			cont := yield(rs, err)
			if !cont || err != nil {
				return
			}
		}
	}
}

func (r *materializedResult) ResultSets(ctx context.Context) xiter.Seq2[result.Set, error] {
	return rangeResultSets(ctx, r)
}

func (r *streamResult) ResultSets(ctx context.Context) xiter.Seq2[result.Set, error] {
	return rangeResultSets(ctx, r)
}

func (r *materializedResult) Close(ctx context.Context) error {
	return nil
}

func (r *materializedResult) NextResultSet(ctx context.Context) (result.Set, error) {
	if r.idx == len(r.resultSets) {
		return nil, io.EOF
	}

	defer func() {
		r.idx++
	}()

	return r.resultSets[r.idx], nil
}

func withStreamResultTrace(t *trace.Query) resultOption {
	return func(s *streamResult) {
		s.trace = t
	}
}

func withIssuesHandler(callback func(issues []*Ydb_Issue.IssueMessage)) resultOption {
	return func(s *streamResult) {
		s.issuesCallback = callback
	}
}

func withStreamResultStatsCallback(callback func(queryStats stats.QueryStats)) resultOption {
	return func(s *streamResult) {
		s.statsCallback = callback
	}
}

func withStreamResultOnClose(onClose func()) resultOption {
	return func(s *streamResult) {
		s.shutdownHooks = append(s.shutdownHooks, onClose)
	}
}

func onNextPartErr(callback func(err error)) resultOption {
	return func(s *streamResult) {
		s.onNextPartErr = append(s.onNextPartErr, callback)
	}
}

func onTxMeta(callback func(txMeta *Ydb_Query.TransactionMeta)) resultOption {
	return func(s *streamResult) {
		s.onTxMeta = append(s.onTxMeta, callback)
	}
}

func withStreamResultCloseTimeout(timeout time.Duration) resultOption {
	return func(s *streamResult) {
		s.closeTimeout = timeout
	}
}

// withStreamCancel wires a CancelFunc that nextPart invokes via
// context.AfterFunc to unblock stream.Recv() when the caller's ctx is
// cancelled. Typically this is the same CancelFunc that controls the gRPC
// context the stream was opened with.
func withStreamCancel(cancel context.CancelFunc) resultOption {
	return func(s *streamResult) {
		s.streamCancel = cancel
	}
}

func newResult(
	ctx context.Context,
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	opts ...resultOption,
) (_ *streamResult, finalErr error) {
	r := streamResult{
		stream:         stream,
		resultSetIndex: -1,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&r)
		}
	}

	if r.trace != nil {
		onDone := trace.QueryOnResultNew(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.newResult"),
		)
		defer func() {
			onDone(finalErr)
		}()
	}

	if err := ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	part, err := r.nextPart(ctx)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		return nil, xerrors.WithStackTrace(err)
	}

	r.lastPart = part

	return &r, nil
}

//nolint:funlen
func (r *streamResult) nextPart(ctx context.Context) (
	part *Ydb_Query.ExecuteQueryResponsePart, finishErr error,
) {
	defer func() {
		if finishErr != nil {
			r.lastErr = finishErr
		}
	}()

	if r.trace != nil {
		onDone := trace.QueryOnResultNextPart(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*streamResult).nextPart"),
		)
		defer func() {
			onDone(part.GetExecStats(), finishErr)
		}()
	}

	if err := ctx.Err(); err != nil {
		if r.streamCancel != nil {
			r.streamCancel()
		}

		return nil, xerrors.WithStackTrace(err)
	}

	if r.lastErr != nil {
		return nil, r.lastErr
	}

	// Forward ctx cancellation to the gRPC stream context so a Recv blocked
	// waiting for the next server message unblocks if the caller cancels.
	// AfterFunc only spawns a goroutine when ctx is actually done, so on the
	// happy path this costs a single register+stop pair (no goroutine, no
	// extra allocations beyond the AfterFunc node itself). The forwarding is
	// strictly per-call: the streamResult intentionally stays decoupled from
	// the original execute() ctx after execute returns, which keeps callers
	// free to iterate the result with a fresh ctx (see CancelAfterExecute).
	if r.streamCancel != nil {
		stop := context.AfterFunc(ctx, r.streamCancel)
		defer stop()
	}

	part, err := nextPart(r.stream)
	if part != nil {
		issues := part.GetIssues()
		if r.issuesCallback != nil && len(issues) > 0 {
			r.issuesCallback(issues)
		}
	}
	if err != nil {
		for _, callback := range r.onNextPartErr {
			callback(err)
		}

		if xerrors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		return nil, xerrors.WithStackTrace(err)
	}

	if txMeta := part.GetTxMeta(); txMeta != nil {
		for _, f := range r.onTxMeta {
			f(txMeta)
		}
	}

	if part.GetExecStats() != nil && r.statsCallback != nil {
		r.statsCallback(stats.FromQueryStats(part.GetExecStats()))
	}

	return part, nil
}

func nextPart(stream Ydb_Query_V1.QueryService_ExecuteQueryClient) (
	part *Ydb_Query.ExecuteQueryResponsePart, err error,
) {
	part, err = stream.Recv()
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		return nil, xerrors.WithStackTrace(err)
	}

	return part, nil
}

func (r *streamResult) Close(ctx context.Context) (finalErr error) {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}

	defer func() {
		for _, f := range r.shutdownHooks {
			f()
		}
	}()

	if r.closeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.closeTimeout)
		defer cancel()
	}

	if r.trace != nil {
		onDone := trace.QueryOnResultClose(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*streamResult).Close"),
		)
		defer func() {
			onDone(finalErr)
		}()
	}

	for {
		if err := ctx.Err(); err != nil {
			return xerrors.WithStackTrace(err)
		}

		_, err := r.nextPart(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				return nil
			}

			return xerrors.WithStackTrace(err)
		}
	}
}

func (r *streamResult) nextResultSet(ctx context.Context) (_ *resultSet, finishErr error) {
	defer func() {
		if finishErr != nil {
			r.lastErr = finishErr
		}
	}()

	nextResultSetIndex := r.resultSetIndex + 1
	for {
		if err := ctx.Err(); err != nil {
			// Mirror nextPart: cancel the gRPC stream context on per-call ctx
			// error so the underlying stream is torn down consistently with
			// lastErr being poisoned by the deferred func above. Without this,
			// nextResultSet would leave the stream half-alive: lastErr is set
			// (so subsequent nextPart/Close(freshCtx) bail out at the lastErr
			// check before ever calling Recv), while the gRPC stream itself
			// keeps holding resources until the server eventually ends it.
			// streamCancel is idempotent, so calling it here is safe even if
			// nextPart later cancels it again.
			if r.streamCancel != nil {
				r.streamCancel()
			}

			return nil, xerrors.WithStackTrace(err)
		}

		if resultSetIndex := r.lastPart.GetResultSetIndex(); resultSetIndex >= nextResultSetIndex {
			r.resultSetIndex = resultSetIndex

			return newResultSet(r.nextPartFunc(ctx, nextResultSetIndex), r.lastPart), nil
		}
		if r.stream == nil {
			return nil, io.EOF
		}
		part, err := r.nextPart(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				return nil, io.EOF
			}

			return nil, xerrors.WithStackTrace(err)
		}
		if part.GetResultSetIndex() < r.resultSetIndex {
			if part.GetResultSetIndex() <= 0 && r.resultSetIndex > 0 {
				return nil, io.EOF
			}

			return nil, xerrors.WithStackTrace(fmt.Errorf(
				"next result set rowIndex %d less than last result set index %d: %w",
				part.GetResultSetIndex(), r.resultSetIndex, errWrongNextResultSetIndex,
			))
		}
		r.lastPart = part
		r.resultSetIndex = part.GetResultSetIndex()
	}
}

func (r *streamResult) nextPartFunc(
	ctx context.Context,
	nextResultSetIndex int64,
) func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
	return func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
		if err := ctx.Err(); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		if r.stream == nil {
			return nil, io.EOF
		}
		part, err := r.nextPart(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				return nil, io.EOF
			}

			return nil, xerrors.WithStackTrace(err)
		}
		r.lastPart = part
		if part.GetResultSetIndex() > nextResultSetIndex {
			return nil, xerrors.WithStackTrace(fmt.Errorf(
				"result set (index=%d) receive part (index=%d) for next result set: %w (%w)",
				nextResultSetIndex, part.GetResultSetIndex(), io.EOF, errReadNextResultSet,
			))
		}

		return part, nil
	}
}

func (r *streamResult) NextResultSet(ctx context.Context) (_ result.Set, err error) {
	if r.trace != nil {
		onDone := trace.QueryOnResultNextResultSet(r.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*streamResult).NextResultSet"),
		)
		defer func() {
			onDone(err)
		}()
	}

	return r.nextResultSet(ctx)
}

func exactlyOneRowFromResult(ctx context.Context, r result.Result) (row result.Row, err error) {
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
		return nil, xerrors.WithStackTrace(ErrMoreThanOneRow)
	case xerrors.Is(err, io.EOF):
		// pass
	default:
		return nil, xerrors.WithStackTrace(err)
	}

	_, err = r.NextResultSet(ctx)
	switch {
	case err == nil:
		return nil, xerrors.WithStackTrace(ErrMoreThanOneRow)
	case xerrors.Is(err, io.EOF):
		// pass
	default:
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func exactlyOneResultSetFromResult(ctx context.Context, r result.Result) (rs result.Set, err error) {
	var rows []query.Row
	rs, err = r.NextResultSet(ctx)
	if err != nil {
		if xerrors.Is(err, io.EOF) {
			return nil, xerrors.WithStackTrace(ErrNoResultSets)
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
		return nil, xerrors.WithStackTrace(ErrMoreThanOneResultSet)
	case xerrors.Is(err, io.EOF):
		// pass
	default:
		return nil, xerrors.WithStackTrace(err)
	}

	return MaterializedResultSet(rs.Index(), rs.Columns(), rs.ColumnTypes(), rows), nil
}

func resultToMaterializedResult(ctx context.Context, r *streamResult) (result.Result, error) {
	type resultSet struct {
		rows    []query.Row
		columns []*Ydb.Column
	}
	resultSetByIndex := make(map[int64]resultSet)

	for {
		curIndex := r.lastPart.GetResultSetIndex()

		rs := resultSetByIndex[curIndex]
		if len(rs.columns) == 0 {
			rs.columns = r.lastPart.GetResultSet().GetColumns()
		}

		for i := range r.lastPart.GetResultSet().GetRows() {
			rs.rows = append(rs.rows, NewRow(rs.columns, r.lastPart.GetResultSet().GetRows()[i]))
		}
		resultSetByIndex[curIndex] = rs

		var err error
		r.lastPart, err = r.nextPart(ctx)
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				break
			}

			return nil, xerrors.WithStackTrace(err)
		}
	}

	resultSets := make([]result.Set, 0, len(resultSetByIndex))
	for rsIndex, rs := range resultSetByIndex {
		columnNames := make([]string, len(rs.columns))
		columnTypes := make([]types.Type, len(rs.columns))

		for i := range rs.columns {
			columnNames[i] = rs.columns[i].GetName()
			columnTypes[i] = types.TypeFromYDB(rs.columns[i].GetType())
		}

		resultSets = append(resultSets, MaterializedResultSet(int(rsIndex), columnNames, columnTypes, rs.rows))
	}
	slices.SortFunc(resultSets, func(a, b result.Set) int {
		return cmp.Compare(a.Index(), b.Index())
	})

	return &materializedResult{
		resultSets: resultSets,
	}, nil
}
