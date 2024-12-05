package ydb_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
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
		a := allocator.New()
		pb, err := params.ToYDB(a)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(exp), fmt.Sprint(pb))
		a.Free()
		t.Run("Raw", func(t *testing.T) {
			params := makeParamsUsingRawProtobuf(t)
			a := allocator.New()
			pb, err := params.ToYDB(a)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprint(exp), fmt.Sprint(pb))
			a.Free()
		})
	})
	t.Run("ParamsFromMap", func(t *testing.T) {
		params := makeParamsUsingParamsFromMap(t)
		a := allocator.New()
		pb, err := params.ToYDB(a)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(exp), fmt.Sprint(pb))
		a.Free()
	})
	t.Run("table/types", func(t *testing.T) {
		params := makeParamsUsingTableTypes(t)
		a := allocator.New()
		pb, err := params.ToYDB(a)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(exp), fmt.Sprint(pb))
		a.Free()
	})
}

func BenchmarkParams(b *testing.B) {
	b.Run("ParamsBuilder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			params := makeParamsUsingParamsBuilder(b)
			a := allocator.New()
			_, _ = params.ToYDB(a)
			a.Free()
		}
	})
	b.Run("RawProtobuf", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			params := makeParamsUsingRawProtobuf(b)
			a := allocator.New()
			_, _ = params.ToYDB(a)
			a.Free()
		}
	})
	b.Run("ParamsFromMap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			params := makeParamsUsingParamsFromMap(b)
			a := allocator.New()
			_, _ = params.ToYDB(a)
			a.Free()
		}
	})
	b.Run("table/types", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			params := makeParamsUsingTableTypes(b)
			a := allocator.New()
			_, _ = params.ToYDB(a)
			a.Free()
		}
	})
}
