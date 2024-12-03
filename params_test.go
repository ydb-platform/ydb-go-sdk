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

func makeParamsUsingBuilder(tb testing.TB) *params.Parameters {
	return ydb.ParamsBuilder().
		Param("$a").Uint64(123).
		Param("$b").Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).
		Param("$c").BeginOptional().Uint64(func(v uint64) *uint64 { return &v }(123)).EndOptional().
		Param("$d").BeginList().Add().Uint64(123).Add().Uint64(123).Add().Uint64(123).Add().Uint64(123).EndList().
		Build()
}

func TestParamsBuilder(t *testing.T) {
	params := makeParamsUsingBuilder(t)
	a := allocator.New()
	v := params.ToYDB(a)
	require.Equal(t,
		fmt.Sprintf("%v", map[string]*Ydb.TypedValue{
			"$a": {
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
			},
			"$b": {
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
			},
			"$c": {
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
			},
			"$d": {
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
			},
		}),
		fmt.Sprintf("%v", v),
	)
	a.Free()
}

func BenchmarkParamsBuilder(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		params := makeParamsUsingBuilder(b)
		a := allocator.New()
		_ = params.ToYDB(a)
		a.Free()
	}
}

func makeParamsUsingParamsMap(tb testing.TB) *params.Parameters {
	params, err := ydb.ParamsFromMap(map[string]any{
		"$a": uint64(123),
		"$b": uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		"$c": func(v uint64) *uint64 { return &v }(123),
		"$d": []uint64{123, 123, 123, 123},
	})
	require.NoError(tb, err)

	return params
}

func TestParamsMap(t *testing.T) {
	params := makeParamsUsingParamsMap(t)
	a := allocator.New()
	v := params.ToYDB(a)
	require.Equal(t,
		fmt.Sprintf("%v", map[string]*Ydb.TypedValue{
			"$a": {
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
			},
			"$b": {
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
			},
			"$c": {
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
			},
			"$d": {
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
			},
		}),
		fmt.Sprintf("%v", v),
	)
	a.Free()
}

func BenchmarkParamsMap(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		params := makeParamsUsingParamsMap(b)
		a := allocator.New()
		_ = params.ToYDB(a)
		a.Free()
	}
}

func makeParamsUsingTypes(tb testing.TB) *params.Parameters {
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

func TestParamsFromTypes(t *testing.T) {
	params := makeParamsUsingTypes(t)
	a := allocator.New()
	v := params.ToYDB(a)
	require.Equal(t,
		fmt.Sprintf("%v", map[string]*Ydb.TypedValue{
			"$a": {
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
			},
			"$b": {
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
			},
			"$c": {
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
			},
			"$d": {
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
			},
		}),
		fmt.Sprintf("%v", v),
	)
	a.Free()
}

func BenchmarkParamsFromTypes(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		params := makeParamsUsingTypes(b)
		a := allocator.New()
		_ = params.ToYDB(a)
		a.Free()
	}
}
