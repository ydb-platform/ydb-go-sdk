package resultset

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestResultAny(t *testing.T) {
	for _, test := range []struct {
		name    string
		columns []options.Column
		values  []types.Value
		exp     []interface{}
	}{
		{
			columns: []options.Column{
				{
					Name:   "column0",
					Type:   types.Optional(types.TypeUint32),
					Family: "family0",
				},
			},
			values: []types.Value{
				types.OptionalValue(types.Uint32Value(43)),
				types.NullValue(types.TypeUint32),
			},
			exp: []interface{}{
				uint32(43),
				nil,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			res := NewResult(
				NewResultSet(
					WithColumns(test.columns...),
					WithValues(test.values...),
				),
			)
			var i int
			var act interface{}
			for res.NextResultSet(context.Background()) {
				for res.NextRow() {
					err := res.ScanWithDefaults(&act)
					if err != nil {
						t.Fatal(err)
					}
					if exp := test.exp[i]; !reflect.DeepEqual(act, exp) {
						t.Errorf(
							"unexpected Any() result: %[1]v (%[1]T); want %[2]v (%[2]T)",
							act, exp,
						)
					}
					i++
				}
			}
			if err := res.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResultOUint32(t *testing.T) {
	for _, test := range []struct {
		name    string
		columns []options.Column
		values  []types.Value
		exp     []uint32
	}{
		{
			columns: []options.Column{
				{
					Name:   "column0",
					Type:   types.Optional(types.TypeUint32),
					Family: "family0",
				},
				{
					Name:   "column1",
					Type:   types.TypeUint32,
					Family: "family0",
				},
			},
			values: []types.Value{
				types.OptionalValue(types.Uint32Value(43)),
				types.Uint32Value(43),
			},
			exp: []uint32{
				43,
				43,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			res := NewResult(
				NewResultSet(
					WithColumns(test.columns...),
					WithValues(test.values...),
				),
			)
			var i int
			var act uint32
			for res.NextResultSet(context.Background()) {
				for res.NextRow() {
					_ = res.ScanWithDefaults(&act)
					if exp := test.exp[i]; !reflect.DeepEqual(act, exp) {
						t.Errorf(
							"unexpected OUint32() result: %[1]v (%[1]T); want %[2]v (%[2]T)",
							act, exp,
						)
					}
					i++
				}
			}
			if err := res.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type resultSetDesc Ydb.ResultSet

type ResultSetOption func(*resultSetDesc)

func WithColumns(cs ...options.Column) ResultSetOption {
	return func(r *resultSetDesc) {
		for _, c := range cs {
			r.Columns = append(r.Columns, &Ydb.Column{
				Name: c.Name,
				Type: value.TypeToYDB(c.Type),
			})
		}
	}
}

func WithValues(vs ...types.Value) ResultSetOption {
	return func(r *resultSetDesc) {
		n := len(r.Columns)
		if n == 0 {
			panic("empty columns")
		}
		if len(vs)%n != 0 {
			panic("malformed values set")
		}
		var row *Ydb.Value
		for i, v := range vs {
			j := i % n
			if j == 0 && i > 0 {
				r.Rows = append(r.Rows, row)
			}
			if j == 0 {
				row = &Ydb.Value{
					Items: make([]*Ydb.Value, n),
				}
			}
			tv := value.ValueToYDB(v)
			act := value.TypeFromYDB(tv.Type)
			exp := value.TypeFromYDB(r.Columns[j].Type)
			if !value.TypesEqual(act, exp) {
				panic(fmt.Sprintf(
					"unexpected types for #%d column: %s; want %s",
					j, act, exp,
				))
			}
			row.Items[j] = tv.Value
		}
		if row != nil {
			r.Rows = append(r.Rows, row)
		}
	}
}

func NewResultSet(opts ...ResultSetOption) *Ydb.ResultSet {
	var d resultSetDesc
	for _, opt := range opts {
		opt(&d)
	}
	return (*Ydb.ResultSet)(&d)
}

func NewResult(sets ...*Ydb.ResultSet) *scanner.Result {
	return &scanner.Result{
		Sets: sets,
	}
}
