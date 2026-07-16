package params

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestVariantStruct(t *testing.T) {
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
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UINT64.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint64Value:  proto.Uint64(123),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Int64",
			itemArgs: []any{int64(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT64.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value:   proto.Int64(123),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Uint32",
			itemArgs: []any{uint32(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UINT32.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value:  proto.Uint32(123),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Int32",
			itemArgs: []any{int32(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT32.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value:   proto.Int32(123),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Uint16",
			itemArgs: []any{uint16(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UINT16.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value:  proto.Uint32(123),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Int16",
			itemArgs: []any{int16(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT16.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value:   proto.Int32(123),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Uint8",
			itemArgs: []any{uint8(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UINT8.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value:  proto.Uint32(123),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Int8",
			itemArgs: []any{int8(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT8.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value:   proto.Int32(123),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Bool",
			itemArgs: []any{true},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_BOOL.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					BoolValue:    proto.Bool(true),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Text",
			itemArgs: []any{"test"},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue:    proto.String("test"),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Bytes",
			itemArgs: []any{[]byte("test")},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_STRING.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					BytesValue:   []byte("test"),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Float",
			itemArgs: []any{float32(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_FLOAT.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					FloatValue:   proto.Float32(float32(123)),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Double",
			itemArgs: []any{float64(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_DOUBLE.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					DoubleValue:  proto.Float64(float64(123)),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Interval",
			itemArgs: []any{time.Second},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INTERVAL.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value:   proto.Int64(1000000),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Datetime",
			itemArgs: []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_DATETIME.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value:  proto.Uint32(123456789),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Date",
			itemArgs: []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_DATE.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value:  proto.Uint32(1428),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Timestamp",
			itemArgs: []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_TIMESTAMP.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint64Value:  proto.Uint64(123456789000000),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Decimal",
			typeArgs: []any{uint32(22), uint32(9)},
			itemArgs: []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, uint32(22), uint32(9)},

			expected: expected{
				Type: Ydb.Type_builder{
					DecimalType: Ydb.DecimalType_builder{
						Precision: 22,
						Scale:     9,
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					High_128:     72623859790382856,
					Low_128:      proto.Uint64(648519454493508870),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "JSON",
			itemArgs: []any{`{"a": 1,"b": "B"}`},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_JSON.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue:    proto.String(`{"a": 1,"b": "B"}`),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "JSONDocument",
			itemArgs: []any{`{"a": 1,"b": "B"}`},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_JSON_DOCUMENT.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue:    proto.String(`{"a": 1,"b": "B"}`),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "YSON",
			itemArgs: []any{[]byte(`{"a": 1,"b": "B"}`)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_YSON.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					BytesValue:   []byte(`{"a": 1,"b": "B"}`),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "Uuid",
			itemArgs: []any{uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UUID.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Low_128:      proto.Uint64(506660481424032516),
					High_128:     1157159078456920585,
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "UUIDWithIssue1501Value",
			itemArgs: []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UUID.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Low_128:      proto.Uint64(651345242494996240),
					High_128:     72623859790382856,
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "TzDatetime",
			itemArgs: []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_TZ_DATETIME.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue:    proto.String("1973-11-29T21:33:09,UTC"),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "TzDate",
			itemArgs: []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_TZ_DATE.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue:    proto.String("1973-11-29,UTC"),
					VariantIndex: 0,
				}.Build(),
			},
		},
		{
			method:   "TzTimestamp",
			itemArgs: []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_TZ_TIMESTAMP.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue:    proto.String("1973-11-29T21:33:09.000000,UTC"),
					VariantIndex: 0,
				}.Build(),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			item := Builder{}.Param("$x").BeginVariant().BeginStruct().Field("key")

			vs, ok := xtest.CallMethod(item, tc.method, tc.typeArgs...)[0].(*variantStruct)
			require.True(t, ok)

			builder, ok := xtest.CallMethod(vs.Name("key"), tc.method, tc.itemArgs...)[0].(*variantStructBuilder)
			require.True(t, ok)

			params := builder.EndStruct().EndVariant().build().toYDB()

			require.Equal(t, xtest.ToJSON(
				map[string]*Ydb.TypedValue{
					"$x": Ydb.TypedValue_builder{
						Type: Ydb.Type_builder{
							VariantType: Ydb.VariantType_builder{
								StructItems: Ydb.StructType_builder{
									Members: []*Ydb.StructMember{
										Ydb.StructMember_builder{
											Name: "key",
											Type: tc.expected.Type,
										}.Build(),
									},
								}.Build(),
							}.Build(),
						}.Build(),
						Value: Ydb.Value_builder{
							NestedValue:  proto.ValueOrDefault(tc.expected.Value),
							VariantIndex: 0,
						}.Build(),
					}.Build(),
				}), xtest.ToJSON(params))
		})
	}
}

func TestVariantStruct_AddFields(t *testing.T) {
	params := Builder{}.Param("$x").BeginVariant().BeginStruct().
		AddFields([]types.StructField{
			{
				Name: "key1",
				T:    types.Bool,
			},
			{
				Name: "key2",
				T:    types.Uint64,
			},
			{
				Name: "key3",
				T:    types.Text,
			},
		}...).Name("key3").Text("Hello, World!").EndStruct().
		EndVariant().build().toYDB()

	require.Equal(t, xtest.ToJSON(
		map[string]*Ydb.TypedValue{
			"$x": Ydb.TypedValue_builder{
				Type: Ydb.Type_builder{
					VariantType: Ydb.VariantType_builder{
						StructItems: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "key1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_BOOL.Enum(),
									}.Build(),
								}.Build(),
								Ydb.StructMember_builder{
									Name: "key2",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UINT64.Enum(),
									}.Build(),
								}.Build(),
								Ydb.StructMember_builder{
									Name: "key3",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UTF8.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					NestedValue: Ydb.Value_builder{
						TextValue: proto.String("Hello, World!"),
					}.Build(),
					VariantIndex: 2,
				}.Build(),
			}.Build(),
		}), xtest.ToJSON(params))
}
