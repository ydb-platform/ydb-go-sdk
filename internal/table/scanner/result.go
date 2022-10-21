package scanner

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/stats"
)

var (
	errAlreadyClosed     = xerrors.Wrap(errors.New("result closed early"))
	errMissingCurrentRow = xerrors.Wrap(errors.New("missing current row"))
)

type baseResult struct {
	scanner

	statsMtx xsync.RWMutex
	stats    *Ydb_TableStats.QueryStats

	closed uint32
}

type streamResult struct {
	baseResult

	recv  func(ctx context.Context) (*Ydb.ResultSet, *Ydb_TableStats.QueryStats, error)
	close func(error) error
}

type unaryResult struct {
	baseResult

	sets    []*Ydb.ResultSet
	nextSet int
}

// Close closes the result, preventing further iteration.
func (r *unaryResult) Close() error {
	if atomic.CompareAndSwapUint32(&r.closed, 0, 1) {
		return nil
	}
	return xerrors.WithStackTrace(errAlreadyClosed)
}

func (r *unaryResult) ResultSetCount() int {
	return len(r.sets)
}

func (r *baseResult) isClosed() bool {
	return atomic.LoadUint32(&r.closed) != 0
}

func (r *baseResult) RowValues() (_ []value.Value, err error) {
	if r.row == nil {
		return nil, xerrors.WithStackTrace(errMissingCurrentRow)
	}
	values := make([]value.Value, len(r.row.GetItems()))
	for i, item := range r.row.GetItems() {
		values[i] = value.FromYDB(r.set.GetColumns()[i].GetType(), item)
	}
	return values, nil
}

type resultWithError interface {
	SetErr(err error)
}

type UnaryResult interface {
	result.Result
	resultWithError
}

type StreamResult interface {
	result.StreamResult
	resultWithError
}

type option func(r *baseResult)

func WithIgnoreTruncated(ignoreTruncated bool) option {
	return func(r *baseResult) {
		r.scanner.ignoreTruncated = ignoreTruncated
	}
}

func WithMarkTruncatedAsRetryable() option {
	return func(r *baseResult) {
		r.scanner.markTruncatedAsRetryable = true
	}
}

func NewStream(
	recv func(ctx context.Context) (*Ydb.ResultSet, *Ydb_TableStats.QueryStats, error),
	onClose func(error) error,
	opts ...option,
) StreamResult {
	r := &streamResult{
		recv:  recv,
		close: onClose,
	}
	for _, o := range opts {
		o(&r.baseResult)
	}
	return r
}

func NewUnary(sets []*Ydb.ResultSet, stats *Ydb_TableStats.QueryStats, opts ...option) UnaryResult {
	r := &unaryResult{
		baseResult: baseResult{
			stats: stats,
		},
		sets: sets,
	}
	for _, o := range opts {
		o(&r.baseResult)
	}
	return r
}

func (r *baseResult) Reset(set *Ydb.ResultSet, columnNames ...string) {
	r.reset(set)
	if set != nil {
		r.setColumnIndexes(columnNames)
	}
}

func (r *unaryResult) NextResultSetErr(ctx context.Context, columns ...string) (err error) {
	if r.isClosed() {
		return xerrors.WithStackTrace(errAlreadyClosed)
	}
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.Reset(r.sets[r.nextSet], columns...)
	r.nextSet++
	return ctx.Err()
}

func (r *unaryResult) NextResultSet(ctx context.Context, columns ...string) bool {
	return r.NextResultSetErr(ctx, columns...) == nil
}

func (r *streamResult) NextResultSetErr(ctx context.Context, columns ...string) (err error) {
	if r.isClosed() {
		return xerrors.WithStackTrace(errAlreadyClosed)
	}
	if err = r.Err(); err != nil {
		return xerrors.WithStackTrace(err)
	}
	s, stats, err := r.recv(ctx)
	if err != nil {
		r.Reset(nil)
		if xerrors.Is(err, io.EOF) {
			return err
		}
		return r.errorf(0, "streamResult.NextResultSetErr(): %w", err)
	}
	r.Reset(s, columns...)
	if stats != nil {
		r.statsMtx.WithLock(func() {
			r.stats = stats
		})
	}
	return ctx.Err()
}

func (r *streamResult) NextResultSet(ctx context.Context, columns ...string) bool {
	return r.NextResultSetErr(ctx, columns...) == nil
}

// CurrentResultSet get current result set
func (r *baseResult) CurrentResultSet() result.Set {
	return r
}

// Stats returns query execution queryStats.
func (r *baseResult) Stats() stats.QueryStats {
	var s queryStats
	r.statsMtx.WithRLock(func() {
		s.stats = r.stats
	})

	if s.stats == nil {
		return nil
	}

	return &s
}

// Close closes the result, preventing further iteration.
func (r *streamResult) Close() (err error) {
	if atomic.CompareAndSwapUint32(&r.closed, 0, 1) {
		return r.close(r.Err())
	}
	return xerrors.WithStackTrace(errAlreadyClosed)
}

func (r *baseResult) inactive() bool {
	return r.isClosed() || r.Err() != nil
}

// HasNextResultSet reports whether result set may be advanced.
// It may be useful to call HasNextResultSet() instead of NextResultSet() to look ahead
// without advancing the result set.
// Note that it does not work with sets from stream.
func (r *streamResult) HasNextResultSet() bool {
	return !r.inactive()
}

// HasNextResultSet reports whether result set may be advanced.
// It may be useful to call HasNextResultSet() instead of NextResultSet() to look ahead
// without advancing the result set.
// Note that it does not work with sets from stream.
func (r *unaryResult) HasNextResultSet() bool {
	if r.inactive() || r.nextSet >= len(r.sets) {
		return false
	}
	return true
}
