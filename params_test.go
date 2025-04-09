package ydb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	a = &Ydb.TypedValue{
		Type: &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: Ydb.Type_UINT64,
			},
		},
		Value: &Ydb.Value{
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: 123,
			},
		},
	}
	b = &Ydb.TypedValue{
		Type: &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: Ydb.Type_UUID,
			},
		},
		Value: &Ydb.Value{
			Value: &Ydb.Value_Low_128{
				Low_128: 506660481424032516,
			},
			High_128: 1157159078456920585,
		},
	}
	c = &Ydb.TypedValue{
		Type: &Ydb.Type{
			Type: &Ydb.Type_OptionalType{
				OptionalType: &Ydb.OptionalType{
					Item: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UINT64,
						},
					},
				},
			},
		},
		Value: &Ydb.Value{
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: 123,
			},
		},
	}
	d = &Ydb.TypedValue{
		Type: &Ydb.Type{
			Type: &Ydb.Type_ListType{
				ListType: &Ydb.ListType{
					Item: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UINT64,
						},
					},
				},
			},
		},
		Value: &Ydb.Value{
			Items: []*Ydb.Value{
				{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: 123,
					},
				},
				{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: 123,
					},
				},
				{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: 123,
					},
				},
				{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: 123,
					},
				},
			},
		},
	}
)

func makeParamsUsingParamsBuilder(tb testing.TB) params.Parameters {
	return ydb.ParamsBuilder().
		Param("$a").Uint64(123).
		Param("$b").Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).
		Param("$c").BeginOptional().Uint64(func(v uint64) *uint64 { return &v }(123)).EndOptional().
		Param("$d").BeginList().Add().Uint64(123).Add().Uint64(123).Add().Uint64(123).Add().Uint64(123).EndList().
		Build()
}

func makeParamsUsingRawProtobuf(tb testing.TB) params.Parameters {
	return ydb.ParamsBuilder().
		Param("$a").Raw(a).
		Param("$b").Raw(b).
		Param("$c").Raw(c).
		Param("$d").Raw(d).
		Build()
}

func makeParamsUsingParamsFromMap(tb testing.TB) params.Parameters {
	return ydb.ParamsFromMap(map[string]any{
		"$a": uint64(123),
		"$b": uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		"$c": func(v uint64) *uint64 { return &v }(123),
		"$d": []uint64{123, 123, 123, 123},
	})
}

func makeParamsUsingTableTypes(tb testing.TB) params.Parameters {
	return table.NewQueryParameters(
		table.ValueParam("$a", types.Uint64Value(123)),
		table.ValueParam("$b", types.UuidValue(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
		table.ValueParam("$c", types.OptionalValue(types.Uint64Value(123))),
		table.ValueParam("$d", types.ListValue(
			types.Uint64Value(123),
			types.Uint64Value(123),
			types.Uint64Value(123),
			types.Uint64Value(123),
		)),
	)
}

func TestParams(t *testing.T) {
	exp := map[string]*Ydb.TypedValue{
		"$a": a,
		"$b": b,
		"$c": c,
		"$d": d,
	}
	t.Run("ParamsBuilder", func(t *testing.T) {
		params := makeParamsUsingParamsBuilder(t)
		pb, err := params.ToYDB()
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(exp), fmt.Sprint(pb))
		t.Run("Raw", func(t *testing.T) {
			params := makeParamsUsingRawProtobuf(t)
			pb, err := params.ToYDB()
			require.NoError(t, err)
			require.Equal(t, fmt.Sprint(exp), fmt.Sprint(pb))
		})
	})
	t.Run("ParamsFromMap", func(t *testing.T) {
		params := makeParamsUsingParamsFromMap(t)
		pb, err := params.ToYDB()
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(exp), fmt.Sprint(pb))
	})
	t.Run("table/types", func(t *testing.T) {
		params := makeParamsUsingTableTypes(t)
		pb, err := params.ToYDB()
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(exp), fmt.Sprint(pb))
	})
}

func BenchmarkParams(b *testing.B) {
	b.Run("ParamsBuilder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			params := makeParamsUsingParamsBuilder(b)
			_, _ = params.ToYDB()
		}
	})
	b.Run("RawProtobuf", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			params := makeParamsUsingRawProtobuf(b)
			_, _ = params.ToYDB()
		}
	})
	b.Run("ParamsFromMap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			params := makeParamsUsingParamsFromMap(b)
			_, _ = params.ToYDB()
		}
	})
	b.Run("table/types", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			params := makeParamsUsingTableTypes(b)
			_, _ = params.ToYDB()
		}
	})
}

func TestParamsFromMap(t *testing.T) {
	t.Run("DefaultTimeTypes", func(t *testing.T) {
		params := ydb.ParamsFromMap(map[string]any{
			"a": time.Unix(123, 456),
			"b": time.Duration(123) * time.Microsecond,
		})
		pp, err := params.ToYDB()
		require.NoError(t, err)
		require.EqualValues(t,
			fmt.Sprintf("%+v", map[string]*Ydb.TypedValue{
				"$a": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_TIMESTAMP,
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 123000000,
						},
					},
				},
				"$b": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_INTERVAL,
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Int64Value{
							Int64Value: 123,
						},
					},
				},
			}),
			fmt.Sprintf("%+v", pp),
		)
	})
	t.Run("BindWideTimeTypes", func(t *testing.T) {
		params := ydb.ParamsFromMap(map[string]any{
			"a": time.Date(1900, 1, 1, 0, 0, 0, 123456, time.UTC),
			"b": time.Duration(123) * time.Nanosecond,
		}, ydb.WithWideTimeTypes(true))
		pp, err := params.ToYDB()
		require.NoError(t, err)
		require.EqualValues(t,
			fmt.Sprintf("%+v", map[string]*Ydb.TypedValue{
				"$a": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_TIMESTAMP64,
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Int64Value{
							Int64Value: -2208988799999877,
						},
					},
				},
				"$b": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_INTERVAL64,
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Int64Value{
							Int64Value: 123,
						},
					},
				},
			}),
			fmt.Sprintf("%+v", pp),
		)
	})
	t.Run("WrongBindings", func(t *testing.T) {
		params := ydb.ParamsFromMap(map[string]any{
			"a": time.Unix(123, 456),
			"b": time.Duration(123),
		}, ydb.WithTablePathPrefix(""))
		pp, err := params.ToYDB()
		require.ErrorIs(t, err, bind.ErrUnsupportedBindingType)
		require.Nil(t, pp)
	})
}
