package ydb_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

func TestParamsBuilder(t *testing.T) {
	t.Run("UsingHelpers", func(t *testing.T) {
		params := ydb.ParamsBuilder().
			Param("test").BeginTuple().Add().Uint64(123).Add().Uint64(321).EndTuple().
			Build()
		require.EqualValues(t,
			map[string]*Ydb.TypedValue{
				"test": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										},
									},
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										},
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
									Uint64Value: 321,
								},
							},
						},
					},
				},
			},
			must(params.ToYDB(allocator.New())),
		)
	})
	t.Run("Raw", func(t *testing.T) {
		params := ydb.ParamsBuilder().Param("test").Raw(&Ydb.TypedValue{
			Type: &Ydb.Type{
				Type: &Ydb.Type_TupleType{
					TupleType: &Ydb.TupleType{
						Elements: []*Ydb.Type{
							{
								Type: &Ydb.Type_TypeId{
									TypeId: Ydb.Type_UINT64,
								},
							},
							{
								Type: &Ydb.Type_TypeId{
									TypeId: Ydb.Type_UINT64,
								},
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
							Uint64Value: 321,
						},
					},
				},
			},
		}).Build()
		require.EqualValues(t,
			map[string]*Ydb.TypedValue{
				"test": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										},
									},
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										},
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
									Uint64Value: 321,
								},
							},
						},
					},
				},
			},
			must(params.ToYDB(allocator.New())),
		)
	})
}

func BenchmarkParamsBuilder(b *testing.B) {
	b.Run("UsingHelpers", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ydb.ParamsBuilder().
				Param("test").BeginTuple().Add().Uint64(123).Add().Uint64(321).EndTuple().
				Build()
		}
	})
	b.Run("Raw", func(b *testing.B) {
		raw := Ydb.TypedValue{
			Type: &Ydb.Type{
				Type: &Ydb.Type_TupleType{
					TupleType: &Ydb.TupleType{
						Elements: []*Ydb.Type{
							{
								Type: &Ydb.Type_TypeId{
									TypeId: Ydb.Type_UINT64,
								},
							},
							{
								Type: &Ydb.Type_TypeId{
									TypeId: Ydb.Type_UINT64,
								},
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
							Uint64Value: 321,
						},
					},
				},
			},
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ydb.ParamsBuilder().Param("test").Raw(&raw).Build()
		}
	})
}
