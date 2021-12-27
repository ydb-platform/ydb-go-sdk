package scanner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	public "github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/stats"
)

var (
	errAlreadyClosed = fmt.Errorf("result already closed")
)

type result struct {
	scanner

	stats *Ydb_TableStats.QueryStats

	closedMtx sync.RWMutex
	closed    bool
}

type streamResult struct {
	result

	ch chan *Ydb.ResultSet
}

type unaryResult struct {
	result

	sets    []*Ydb.ResultSet
	nextSet int
}

func (r *unaryResult) ResultSetCount() int {
	return len(r.sets)
}

func (r *result) isClosed() bool {
	r.closedMtx.RLock()
	defer r.closedMtx.RUnlock()
	return r.closed
}

func (r *result) UpdateStats(stats *Ydb_TableStats.QueryStats) {
	r.stats = stats
}

func (r *streamResult) Append(set *Ydb.ResultSet) {
	if r.isClosed() {
		return
	}
	r.ch <- set
}

type resultWithError interface {
	SetErr(err error)
}

type UnaryResult interface {
	public.Result
	resultWithError
}

type StreamResult interface {
	public.StreamResult
	resultWithError

	Append(set *Ydb.ResultSet)
	UpdateStats(stats *Ydb_TableStats.QueryStats)
}

func NewStream() StreamResult {
	r := &streamResult{
		ch: make(chan *Ydb.ResultSet, 1),
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
	r.setColumnIndexes(columnNames)
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
	select {
	case s, ok := <-r.ch:
		if !ok || s == nil {
			return false
		}
		r.Reset(s, columns...)
		return true

	case <-ctx.Done():
		r.errMtx.Lock()
		if r.err == nil {
			r.err = ctx.Err()
		}
		r.errMtx.Unlock()
		r.Reset(nil)
		return false
	}
}

// CurrentResultSet get current result set
func (r *result) CurrentResultSet() public.Set {
	return r
}

// Stats returns query execution queryStats.
func (r *result) Stats() stats.QueryStats {
	var s queryStats
	s.stats = r.stats
	s.processCPUTime = time.Microsecond * time.Duration(r.stats.GetProcessCpuTimeUs())
	s.pos = 0
	return &s
}

// Close closes the result, preventing further iteration.
func (r *result) Close() error {
	r.closedMtx.Lock()
	defer r.closedMtx.Unlock()
	if r.closed {
		return errAlreadyClosed
	}
	r.closed = true
	return nil
}

// Close closes the result, preventing further iteration.
func (r *streamResult) Close() (err error) {
	if err = r.result.Close(); err != nil {
		return err
	}
	if r.ch != nil {
		close(r.ch)
	}
	return nil
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

///---------------non-stream-----------------/>
