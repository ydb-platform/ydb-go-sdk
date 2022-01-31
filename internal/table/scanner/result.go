package scanner

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/stats"
)

type result struct {
	scanner

	statsMtx sync.RWMutex
	stats    *Ydb_TableStats.QueryStats

	closedMtx sync.RWMutex
	closed    bool
}

type streamResult struct {
	result

	recv  func(ctx context.Context) (*Ydb.ResultSet, *Ydb_TableStats.QueryStats, error)
	close func(error) error
}

type unaryResult struct {
	result

	sets    []*Ydb.ResultSet
	nextSet int
}

// Close closes the result, preventing further iteration.
func (r *unaryResult) Close() error {
	r.closedMtx.Lock()
	defer r.closedMtx.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return nil
}

func (r *unaryResult) ResultSetCount() int {
	return len(r.sets)
}

func (r *result) isClosed() bool {
	r.closedMtx.RLock()
	defer r.closedMtx.RUnlock()
	return r.closed
}

type resultWithError interface {
	SetErr(err error)
}

type UnaryResult interface {
	ydb_table_result.Result
	resultWithError
}

type StreamResult interface {
	ydb_table_result.StreamResult
	resultWithError
}

func NewStream(
	recv func(ctx context.Context) (*Ydb.ResultSet, *Ydb_TableStats.QueryStats, error),
	onClose func(error) error,
) StreamResult {
	r := &streamResult{
		recv:  recv,
		close: onClose,
	}
	return r
}

func NewUnary(sets []*Ydb.ResultSet, stats *Ydb_TableStats.QueryStats) UnaryResult {
	r := &unaryResult{
		result: result{
			stats: stats,
		},
		sets: sets,
	}
	return r
}

func (r *result) Reset(set *Ydb.ResultSet, columnNames ...string) {
	r.reset(set)
	if set != nil {
		r.setColumnIndexes(columnNames)
	}
}

// NextResultSet selects next result set in the result.
// columns - names of columns in the resultSet that will be scanned
// It returns false if there are no more result sets.
// Stream sets are supported.
func (r *unaryResult) NextResultSet(ctx context.Context, columns ...string) bool {
	if !r.HasNextResultSet() {
		return false
	}
	r.Reset(r.sets[r.nextSet], columns...)
	r.nextSet++
	return true
}

// NextResultSet selects next result set in the result.
// columns - names of columns in the resultSet that will be scanned
// It returns false if there are no more result sets.
// Stream sets are supported.
func (r *streamResult) NextResultSet(ctx context.Context, columns ...string) bool {
	if r.inactive() {
		return false
	}
	s, stats, err := r.recv(ctx)
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		r.errMtx.Lock()
		if r.err == nil {
			r.err = ctx.Err()
		}
		r.errMtx.Unlock()
		r.Reset(nil)
		return false
	}
	r.Reset(s, columns...)
	if stats != nil {
		r.statsMtx.Lock()
		r.stats = stats
		r.statsMtx.Unlock()
	}
	return true
}

// CurrentResultSet get current result set
func (r *result) CurrentResultSet() ydb_table_result.Set {
	return r
}

// Stats returns query execution queryStats.
func (r *result) Stats() ydb_table_stats.QueryStats {
	var s queryStats
	r.statsMtx.RLock()
	s.stats = r.stats
	r.statsMtx.RUnlock()
	s.processCPUTime = time.Microsecond * time.Duration(s.stats.GetProcessCpuTimeUs())
	s.pos = 0
	return &s
}

// Close closes the result, preventing further iteration.
func (r *streamResult) Close() (err error) {
	r.closedMtx.Lock()
	defer r.closedMtx.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.close(r.Err())
}

func (r *result) inactive() bool {
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
