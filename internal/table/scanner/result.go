package scanner

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type ResultSet interface {
	// ColumnCount returns number of columns in the current result set.
	ColumnCount() int

	// Columns allows to iterate over all columns of the current result set.
	Columns(it func(options.Column))

	// RowCount returns number of rows in the result set.
	RowCount() int

	// ItemCount returns number of items in the current row.
	ItemCount() int

	// Truncated returns true if current result set has been truncated by server
	Truncated() bool
}

type Result struct {
	scanner

	Sets       []*Ydb.ResultSet
	QueryStats *Ydb_TableStats.QueryStats

	SetCh       chan *Ydb.ResultSet
	SetChErr    *error
	SetChCancel func()

	nextSet int
	closed  bool
}

// NextResultSet selects next result set in the result.
// columns - names of columns in the resultSet that will be scanned
// It returns false if there are no more result sets.
// Stream sets are supported.
func (r *Result) NextResultSet(ctx context.Context, columns ...string) bool {
	if !r.HasNextResultSet() {
		return r.nextStreamSet(ctx, columns...)
	}
	r.reset(r.Sets[r.nextSet], columns...)
	r.nextSet++
	return true
}

// CurrentResultSet get current result set
func (r *Result) CurrentResultSet() ResultSet {
	return r
}

// Stats returns query execution QueryStats.
func (r *Result) Stats() QueryStats {
	var s QueryStats
	s.stats = r.QueryStats
	s.processCPUTime = time.Microsecond * time.Duration(r.QueryStats.GetProcessCpuTimeUs())
	s.pos = 0
	return s
}

// Close closes the Result, preventing further iteration.
func (r *Result) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.SetCh != nil {
		r.SetChCancel()
	}
	return nil
}

func (r *Result) inactive() bool {
	return r.closed || r.err != nil
}

// NextStreamSet selects next result set from the result of streaming operation.
// columns - names of columns in the resultSet that will be scanned
// It returns false if stream is closed or ctx is canceled.
// Note that in case of context cancelation it marks via error set.
func (r *Result) nextStreamSet(ctx context.Context, columns ...string) bool {
	if r.inactive() || r.SetCh == nil {
		return false
	}
	select {
	case s, ok := <-r.SetCh:
		if !ok {
			if r.SetChErr != nil {
				r.err = *r.SetChErr
			}
			return false
		}
		r.reset(s, columns...)
		return true

	case <-ctx.Done():
		if r.err == nil {
			r.err = ctx.Err()
		}
		r.reset(nil)
		return false
	}
}

///<--------------non-stream-----------------

// ResultSetCount returns number of result sets.
// Note that it does not work if r is the result of streaming operation.
func (r *Result) ResultSetCount() int {
	return len(r.Sets)
}

// TotalRowCount returns the number of rows among the all result sets.
// Note that it does not work if r is the result of streaming operation.
func (r *Result) TotalRowCount() (n int) {
	for _, s := range r.Sets {
		n += len(s.Rows)
	}
	return
}

// HasNextResultSet reports whether result set may be advanced.
// It may be useful to call HasNextResultSet() instead of NextResultSet() to look ahead
// without advancing the result set.
// Note that it does not work with sets from stream.
func (r *Result) HasNextResultSet() bool {
	if r.inactive() || r.nextSet == len(r.Sets) {
		return false
	}
	return true
}

///---------------non-stream-----------------/>
