package table

import (
	"context"

	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_TableStats"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/result"
)

// Result is a result of a query.
//
// Use NextSet(), NextRow() and NextItem() to advance through the result sets,
// its rows and row's items.
//
//     res, err := s.Execute(ctx, txc, "SELECT ...")
//     defer res.Close()
//     for res.NextSet() {
//         for res.NextRow() {
//             var id int64
//             var name string
//             res.NextItem()
//             id = res.OInt64()  // Optional<Int64> type.
//             name = res.OUTF8() // Optional<Utf8> type.
//         }
//     }
//     if err := res.Err() { // get any error encountered during iteration
//         // handle error
//     }
//
// Note that value getters (res.OInt64() and res.OUTF8() as in the example
// above) may fail the result iteration. That is, if current value under scan
// is not of requested type, then appropriate zero value will be returned from
// getter and res.Err() become non-nil. After that, NextSet(), NextRow() and
// NextItem() will return false.
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
	if r.inactive() || r.nextSet == len(r.sets) {
		return false
	}
	return true
}

// NextSet selects next result set in the result.
// It returns false if there are no more result sets.
func (r *Result) NextSet() bool {
	if !r.HasNextSet() {
		return false
	}
	result.Reset(&r.Scanner, r.sets[r.nextSet])
	r.nextSet++
	return true
}

// Truncated returns true if current result set has been truncated by server.
func (r *Result) Truncated() bool {
	return r.Scanner.ResultSetTruncated()
}

// NextStreamSet selects next result set from the result of streaming operation.
// It returns false if stream is closed or ctx is canceled.
// Note that in case of context cancelation it does not marks whole result as
// failed.
func (r *Result) NextStreamSet(ctx context.Context) bool {
	if r.inactive() || r.setCh == nil {
		return false
	}
	select {
	case s, ok := <-r.setCh:
		if !ok {
			r.err = *r.setChErr
			return false
		}
		result.Reset(&r.Scanner, s)
		return true

	case <-ctx.Done():
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
