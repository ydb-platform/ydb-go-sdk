package scanner

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestResultAny(t *testing.T) {
	for _, test := range []struct {
		name    string
		columns []options.Column
		values  []value.Value
		exp     []interface{}
	}{
		{
			columns: []options.Column{
				{
					Name:   "column0",
					Type:   types.NewOptional(types.Uint32),
					Family: "family0",
				},
			},
			values: []value.Value{
				value.OptionalValue(value.Uint32Value(43)),
				value.NullValue(types.Uint32),
			},
			exp: []interface{}{
				uint32(43),
				nil,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			res := NewUnary(
				[]*Ydb.ResultSet{
					NewResultSet(
						WithColumns(test.columns...),
						WithValues(test.values...),
					),
				},
				nil,
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
		values  []value.Value
		exp     []uint32
	}{
		{
			columns: []options.Column{
				{
					Name:   "column0",
					Type:   types.NewOptional(types.Uint32),
					Family: "family0",
				},
				{
					Name:   "column1",
					Type:   types.Uint32,
					Family: "family0",
				},
			},
			values: []value.Value{
				value.OptionalValue(value.Uint32Value(43)),
				value.Uint32Value(43),
			},
			exp: []uint32{
				43,
				43,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			res := NewUnary(
				[]*Ydb.ResultSet{
					NewResultSet(
						WithColumns(test.columns...),
						WithValues(test.values...),
					),
				},
				nil,
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
				Type: types.TypeToYDB(c.Type),
			})
		}
	}
}

func WithValues(vs ...value.Value) ResultSetOption {
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
			tv := value.ToYDB(v)
			act := types.TypeFromYDB(tv.GetType())
			exp := types.TypeFromYDB(r.Columns[j].GetType())
			if !types.Equal(act, exp) {
				panic(fmt.Sprintf(
					"unexpected types for #%d column: %s; want %s",
					j, act, exp,
				))
			}
			row.Items[j] = tv.GetValue()
		}
		if row != nil {
			r.Rows = append(r.Rows, row)
		}
	}
}

func NewResultSet(opts ...ResultSetOption) *Ydb.ResultSet {
	var d resultSetDesc
	for _, opt := range opts {
		if opt != nil {
			opt(&d)
		}
	}

	return (*Ydb.ResultSet)(&d)
}

func TestNewStreamWithRecvFirstResultSet(t *testing.T) {
	for _, tt := range []struct {
		ctx         context.Context //nolint:containedctx
		recvCounter int
		err         error
	}{
		{
			ctx: context.Background(),
			err: nil,
		},
		{
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				return ctx
			}(),
			err: context.Canceled,
		},
		{
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				cancel()

				return ctx
			}(),
			err: context.DeadlineExceeded,
		},
		{
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				cancel()

				return ctx
			}(),
			err: context.Canceled,
		},
	} {
		t.Run("", func(t *testing.T) {
			result, err := NewStream(tt.ctx,
				func(ctx context.Context) (*Ydb.ResultSet, *Ydb_TableStats.QueryStats, error) {
					tt.recvCounter++
					if tt.recvCounter > 1000 {
						return nil, nil, io.EOF
					}

					return &Ydb.ResultSet{}, nil, ctx.Err()
				},
				func(err error) error {
					return err
				},
			)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.EqualValues(t, 1, tt.recvCounter)
				require.EqualValues(t, 1, result.(*streamResult).nextResultSetCounter.Load())
				for i := range make([]struct{}, 1000) {
					err = result.NextResultSetErr(tt.ctx)
					require.NoError(t, err)
					require.Equal(t, i+1, tt.recvCounter)
					require.Equal(t, i+2, int(result.(*streamResult).nextResultSetCounter.Load()))
				}
				err = result.NextResultSetErr(tt.ctx)
				require.ErrorIs(t, err, io.EOF)
				require.True(t, err == io.EOF) //nolint:errorlint,testifylint
				require.Equal(t, 1001, tt.recvCounter)
				require.Equal(t, 1002, int(result.(*streamResult).nextResultSetCounter.Load()))
			}
		})
	}
}
