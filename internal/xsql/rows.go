package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	_ driver.Rows              = &rows{}
	_ driver.RowsNextResultSet = &rows{}
	_ driver.Rows              = &single{}

	_ types.Scanner = &valuer{}
)

type rows struct {
	conn    *conn
	result  result.BaseResult
	nextSet sync.Once
}

func (r *rows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r *rows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

func (r *rows) Columns() []string {
	r.nextSet.Do(func() {
		r.result.NextResultSet(context.Background())
	})
	var i int
	cs := make([]string, r.result.CurrentResultSet().ColumnCount())
	r.result.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

func (r *rows) NextResultSet() error {
	r.nextSet.Do(func() {})
	if !r.result.NextResultSet(context.Background()) {
		return io.EOF
	}
	return nil
}

func (r *rows) HasNextResultSet() bool {
	return r.result.HasNextResultSet()
}

func (r *rows) Next(dst []driver.Value) (err error) {
	r.nextSet.Do(func() {
		r.result.NextResultSet(context.Background())
	})
	if !r.result.NextRow() {
		return io.EOF
	}
	if err = r.result.Err(); err != nil {
		return r.conn.checkClosed(err)
	}
	values := make([]indexed.RequiredOrOptional, len(dst))
	for i := range dst {
		values[i] = &valuer{}
	}
	if err = r.result.Scan(values...); err != nil {
		return r.conn.checkClosed(err)
	}
	for i := range values {
		dst[i] = values[i].(*valuer).Value()
	}
	if err = r.result.Err(); err != nil {
		return r.conn.checkClosed(err)
	}
	return nil
}

func (r *rows) Close() error {
	return r.result.Close()
}

type valuer struct {
	v interface{}
}

func (p *valuer) UnmarshalYDB(raw types.RawValue) error {
	p.v = raw.Any()
	return nil
}

func (p *valuer) Value() interface{} {
	return p.v
}

type single struct {
	values []sql.NamedArg
}

func (r *single) Columns() (columns []string) {
	for _, v := range r.values {
		columns = append(columns, v.Name)
	}
	return columns
}

func (r *single) Close() error {
	return nil
}

func (r *single) Next(dst []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.values = nil
	return nil
}

type nopResult struct{}

func (r nopResult) LastInsertId() (int64, error) {
	return 0, ErrUnsupported
}

func (r nopResult) RowsAffected() (int64, error) {
	return 0, ErrUnsupported
}

type namedValueChecker struct{}

func (namedValueChecker) CheckNamedValue(v *driver.NamedValue) (err error) {
	if v.Name == "" {
		return fmt.Errorf("ydb: only named parameters are supported")
	}

	if valuer, ok := v.Value.(driver.Valuer); ok {
		v.Value, err = valuer.Value()
		if err != nil {
			return fmt.Errorf("ydb: driver.Valuer error: %w", err)
		}
	}

	switch x := v.Value.(type) {
	case types.Value:
		// OK.
	case bool:
		v.Value = types.BoolValue(x)
	case int8:
		v.Value = types.Int8Value(x)
	case uint8:
		v.Value = types.Uint8Value(x)
	case int16:
		v.Value = types.Int16Value(x)
	case uint16:
		v.Value = types.Uint16Value(x)
	case int32:
		v.Value = types.Int32Value(x)
	case uint32:
		v.Value = types.Uint32Value(x)
	case int64:
		v.Value = types.Int64Value(x)
	case uint64:
		v.Value = types.Uint64Value(x)
	case float32:
		v.Value = types.FloatValue(x)
	case float64:
		v.Value = types.DoubleValue(x)
	case []byte:
		v.Value = types.StringValue(x)
	case string:
		v.Value = types.UTF8Value(x)
	case [16]byte:
		v.Value = types.UUIDValue(x)

	default:
		return xerrors.WithStackTrace(fmt.Errorf("ydb: unsupported type: %T", x))
	}

	return nil
}
