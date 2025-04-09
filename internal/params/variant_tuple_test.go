package params

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestVariantTuple(t *testing.T) {
	type expected struct {
		Type  *Ydb.Type
		Value *Ydb.Value
	}

	tests := []struct {
		method string

		typeArgs []any
		itemArgs []any

		expected expected
	}{
		{
			method:   "Uint64",
			itemArgs: []any{uint64(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: 123,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Int64",
			itemArgs: []any{int64(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int64Value{
						Int64Value: 123,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Uint32",
			itemArgs: []any{uint32(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT32},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 123,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Int32",
			itemArgs: []any{int32(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int32Value{
						Int32Value: 123,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Uint16",
			itemArgs: []any{uint16(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT16},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 123,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Int16",
			itemArgs: []any{int16(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT16},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int32Value{
						Int32Value: 123,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Uint8",
			itemArgs: []any{uint8(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT8},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 123,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Int8",
			itemArgs: []any{int8(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT8},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int32Value{
						Int32Value: 123,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Bool",
			itemArgs: []any{true},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_BoolValue{
						BoolValue: true,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Text",
			itemArgs: []any{"test"},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: "test",
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Bytes",
			itemArgs: []any{[]byte("test")},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_BytesValue{
						BytesValue: []byte("test"),
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Float",
			itemArgs: []any{float32(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_FloatValue{
						FloatValue: float32(123),
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Double",
			itemArgs: []any{float64(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_DoubleValue{
						DoubleValue: float64(123),
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Interval",
			itemArgs: []any{time.Second},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INTERVAL},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int64Value{
						Int64Value: 1000000,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Datetime",
			itemArgs: []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATETIME},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 123456789,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Date",
			itemArgs: []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 1428,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Timestamp",
			itemArgs: []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: 123456789000000,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Decimal",
			typeArgs: []any{uint32(22), uint32(9)},
			itemArgs: []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, uint32(22), uint32(9)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_DecimalType{
						DecimalType: &Ydb.DecimalType{
							Precision: 22,
							Scale:     9,
						},
					},
				},
				Value: &Ydb.Value{
					High_128: 72623859790382856,
					Value: &Ydb.Value_Low_128{
						Low_128: 648519454493508870,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "JSON",
			itemArgs: []any{`{"a": 1,"b": "B"}`},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: `{"a": 1,"b": "B"}`,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "JSONDocument",
			itemArgs: []any{`{"a": 1,"b": "B"}`},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON_DOCUMENT},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: `{"a": 1,"b": "B"}`,
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "YSON",
			itemArgs: []any{[]byte(`{"a": 1,"b": "B"}`)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_YSON},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_BytesValue{
						BytesValue: []byte(`{"a": 1,"b": "B"}`),
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "Uuid",
			itemArgs: []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UUID},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Low_128{
						Low_128: 506660481424032516,
					},
					High_128:     1157159078456920585,
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "UUIDWithIssue1501Value",
			itemArgs: []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UUID},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Low_128{
						Low_128: 651345242494996240,
					},
					High_128:     72623859790382856,
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "TzDatetime",
			itemArgs: []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATETIME},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: "1973-11-29T21:33:09,UTC",
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "TzDate",
			itemArgs: []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATE},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: "1973-11-29,UTC",
					},
					VariantIndex: 0,
				},
			},
		},
		{
			method:   "TzTimestamp",
			itemArgs: []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_TIMESTAMP},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: "1973-11-29T21:33:09.000000,UTC",
					},
					VariantIndex: 0,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			item := Builder{}.Param("$x").BeginVariant().BeginTuple().Types()

			types, ok := xtest.CallMethod(item, tc.method, tc.typeArgs...)[0].(*variantTupleTypes)
			require.True(t, ok)

			builder, ok := xtest.CallMethod(types.Index(0), tc.method, tc.itemArgs...)[0].(*variantTupleBuilder)
			require.True(t, ok)

			params := builder.EndTuple().EndVariant().build().toYDB()

			require.Equal(t, xtest.ToJSON(
				map[string]*Ydb.TypedValue{
					"$x": {
						Type: &Ydb.Type{
							Type: &Ydb.Type_VariantType{
								VariantType: &Ydb.VariantType{
									Type: &Ydb.VariantType_TupleItems{
										TupleItems: &Ydb.TupleType{
											Elements: []*Ydb.Type{
												tc.expected.Type,
											},
										},
									},
								},
							},
						},
						Value: &Ydb.Value{
							Value: &Ydb.Value_NestedValue{
								NestedValue: tc.expected.Value,
							},
						},
					},
				}), xtest.ToJSON(params))
		})
	}
}

func TestVariantTuple_AddTypes(t *testing.T) {
	params := Builder{}.Param("$x").BeginVariant().BeginTuple().
		Types().AddTypes(types.Int64, types.Bool).
		Index(1).
		Bool(true).
		EndTuple().EndVariant().build().toYDB()

	require.Equal(t, xtest.ToJSON(
		map[string]*Ydb.TypedValue{
			"$x": {
				Type: &Ydb.Type{
					Type: &Ydb.Type_VariantType{
						VariantType: &Ydb.VariantType{
							Type: &Ydb.VariantType_TupleItems{
								TupleItems: &Ydb.TupleType{
									Elements: []*Ydb.Type{
										{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_INT64,
											},
										},
										{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_BOOL,
											},
										},
									},
								},
							},
						},
					},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_NestedValue{
						NestedValue: &Ydb.Value{
							Value: &Ydb.Value_BoolValue{
								BoolValue: true,
							},
						},
					},
					VariantIndex: 1,
				},
			},
		}), xtest.ToJSON(params))
}
