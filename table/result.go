package table

import (
	"context"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_TableStats"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal"
	"github.com/YandexDatabase/ydb-go-sdk/v2/internal/result"
)

// Result is a result of a query.
//
// Use NextSet(), NextRow() and Scan() to advance through the result sets,
// its rows and row's items.
//
//     res, err := s.Execute(ctx, txc, "SELECT ...")
//     defer res.Close()
//     for res.NextSet() {
//         for res.NextRow() {
//             var id int64
//             var name *string //optional value
//             res.Scan(&id,&name)
//         }
//     }
//     if err := res.Err() { // get any error encountered during iteration
//         // handle error
//     }
//
// If current value under scan
// is not requested type, then res.Err() become non-nil.
// After that, NextSet(), NextRow() will return false.
type Result struct {
	result.Scanner

	sets    []*Ydb.ResultSet
	nextSet int

	stats *Ydb_TableStats.QueryStats

	setCh       chan *Ydb.ResultSet
	setChErr    *error
	setChCancel func()

	err    error
	closed bool
}

// Stats returns query execution stats.
func (r *Result) Stats() (stats QueryStats) {
	stats.init(r.stats)
	return
}

// SetCount returns number of result sets.
// Note that it does not work if r is the result of streaming operation.
func (r *Result) SetCount() int {
	return len(r.sets)
}

// RowCount returns the number of rows among the all result sets.
func (r *Result) RowCount() (n int) {
	for _, s := range r.sets {
		n += len(s.Rows)
	}
	return
}

// SetRowCount returns number of rows in the current result set.
func (r *Result) SetRowCount() int {
	return r.Scanner.RowCount()
}

// SetRowItemCount returns number of items in the current row.
func (r *Result) SetRowItemCount() int {
	return r.Scanner.ItemCount()
}

// Close closes the Result, preventing further iteration.
func (r *Result) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.setCh != nil {
		r.setChCancel()
	}
	return nil
}

func (r *Result) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.Scanner.Err()
}

func (r *Result) inactive() bool {
	return r.closed || r.err != nil || r.Scanner.Err() != nil
}

// HasNextSet reports whether result set may be advanced.
//
// It may be useful to call HasNextSet() instead of NextSet() to look ahead
// without advancing the result set.
//
// Note that it does not work with sets from stream.
func (r *Result) HasNextSet() bool {
	if r.setCh != nil {
		panic("HasNextSet must be called only from non streaming operation")
	}
	if r.inactive() || r.nextSet == len(r.sets) {
		return false
	}
	return true
}

// NextSet selects next result set in the result.
// columns - names of columns in the resultSet that will be scanned
// It returns false if there are no more result sets.
func (r *Result) NextSet(columns ...string) bool {
	if r.setCh != nil {
		panic("NextSet must be called only from non streaming operation")
	}
	if !r.HasNextSet() {
		return false
	}
	result.Reset(&r.Scanner, r.sets[r.nextSet], columns...)
	r.nextSet++
	return true
}

// Truncated returns true if current result set has been truncated by server.
func (r *Result) Truncated() bool {
	return r.Scanner.ResultSetTruncated()
}

// NextStreamSet selects next result set from the result of streaming operation.
// columns - names of columns in the resultSet that will be scanned
// It returns false if stream is closed or ctx is canceled.
// Note that in case of context cancelation it marks via error set.
func (r *Result) NextStreamSet(ctx context.Context, columns ...string) bool {
	if r.setCh == nil {
		panic("NextStreamSet must be called only from streaming operation")
	}
	if r.inactive() {
		return false
	}
	select {
	case s, ok := <-r.setCh:
		if !ok {
			if r.setChErr != nil {
				r.err = *r.setChErr
			}
			return false
		}
		result.Reset(&r.Scanner, s, columns...)
		return true

	case <-ctx.Done():
		if r.err == nil {
			r.err = ctx.Err()
		}
		result.Reset(&r.Scanner, nil)
		return false
	}
}

// Columns allows to iterate over all columns of the current result set.
func (r *Result) Columns(it func(Column)) {
	result.Columns(&r.Scanner, func(name string, typ internal.T) {
		it(Column{
			Name: name,
			Type: typ,
		})
	})
}
