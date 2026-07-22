package params

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestSet(t *testing.T) {
	type expected struct {
		Type  *Ydb.Type
		Value *Ydb.Value
	}

	tests := []struct {
		method string
		args   []any

		expected expected
	}{
		{
			method: "Uint64",
			args:   []any{uint64(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UINT64.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint64Value: proto.Uint64(123),
				}.Build(),
			},
		},
		{
			method: "Int64",
			args:   []any{int64(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT64.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value: proto.Int64(123),
				}.Build(),
			},
		},
		{
			method: "Uint32",
			args:   []any{uint32(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UINT32.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(123),
				}.Build(),
			},
		},
		{
			method: "Int32",
			args:   []any{int32(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT32.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value: proto.Int32(123),
				}.Build(),
			},
		},
		{
			method: "Uint16",
			args:   []any{uint16(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UINT16.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(123),
				}.Build(),
			},
		},
		{
			method: "Int16",
			args:   []any{int16(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT16.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value: proto.Int32(123),
				}.Build(),
			},
		},
		{
			method: "Uint8",
			args:   []any{uint8(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UINT8.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(123),
				}.Build(),
			},
		},
		{
			method: "Int8",
			args:   []any{int8(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INT8.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value: proto.Int32(123),
				}.Build(),
			},
		},
		{
			method: "Bool",
			args:   []any{true},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_BOOL.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					BoolValue: proto.Bool(true),
				}.Build(),
			},
		},
		{
			method: "Text",
			args:   []any{"test"},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UTF8.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("test"),
				}.Build(),
			},
		},
		{
			method: "Bytes",
			args:   []any{[]byte("test")},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_STRING.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					BytesValue: []byte("test"),
				}.Build(),
			},
		},
		{
			method: "Float",
			args:   []any{float32(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_FLOAT.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					FloatValue: proto.Float32(float32(123)),
				}.Build(),
			},
		},
		{
			method: "Double",
			args:   []any{float64(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_DOUBLE.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					DoubleValue: proto.Float64(float64(123)),
				}.Build(),
			},
		},
		{
			method: "Interval",
			args:   []any{time.Second},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_INTERVAL.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value: proto.Int64(1000000),
				}.Build(),
			},
		},
		{
			method: "Datetime",
			args:   []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_DATETIME.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(123456789),
				}.Build(),
			},
		},
		{
			method: "Date",
			args:   []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_DATE.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(1428),
				}.Build(),
			},
		},
		{
			method: "Timestamp",
			args:   []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_TIMESTAMP.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint64Value: proto.Uint64(123456789000000),
				}.Build(),
			},
		},
		{
			method: "Decimal",
			args:   []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, uint32(22), uint32(9)},

			expected: expected{
				Type: Ydb.Type_builder{
					DecimalType: Ydb.DecimalType_builder{
						Precision: 22,
						Scale:     9,
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					High_128: 72623859790382856,
					Low_128:  proto.Uint64(648519454493508870),
				}.Build(),
			},
		},
		{
			method: "JSON",
			args:   []any{`{"a": 1,"b": "B"}`},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_JSON.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String(`{"a": 1,"b": "B"}`),
				}.Build(),
			},
		},
		{
			method: "JSONDocument",
			args:   []any{`{"a": 1,"b": "B"}`},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_JSON_DOCUMENT.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String(`{"a": 1,"b": "B"}`),
				}.Build(),
			},
		},
		{
			method: "YSON",
			args:   []any{[]byte(`{"a": 1,"b": "B"}`)},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_YSON.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					BytesValue: []byte(`{"a": 1,"b": "B"}`),
				}.Build(),
			},
		},
		{
			method: "Uuid",
			args:   []any{uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UUID.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Low_128:  proto.Uint64(506660481424032516),
					High_128: 1157159078456920585,
				}.Build(),
			},
		},
		{
			method: "UUIDWithIssue1501Value",
			args:   []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_UUID.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					Low_128:  proto.Uint64(651345242494996240),
					High_128: 72623859790382856,
				}.Build(),
			},
		},
		{
			method: "TzDatetime",
			args:   []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_TZ_DATETIME.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("1973-11-29T21:33:09,UTC"),
				}.Build(),
			},
		},
		{
			method: "TzDate",
			args:   []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_TZ_DATE.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("1973-11-29,UTC"),
				}.Build(),
			},
		},
		{
			method: "TzTimestamp",
			args:   []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: Ydb.Type_builder{
					TypeId: Ydb.Type_TZ_TIMESTAMP.Enum(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("1973-11-29T21:33:09.000000,UTC"),
				}.Build(),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			item := Builder{}.Param("$x").BeginSet().Add()

			result, ok := xtest.CallMethod(item, tc.method, tc.args...)[0].(*set)
			require.True(t, ok)

			params := result.EndSet().build().toYDB()
			require.Equal(t, xtest.ToJSON(
				map[string]*Ydb.TypedValue{
					"$x": Ydb.TypedValue_builder{
						Type: Ydb.Type_builder{
							DictType: Ydb.DictType_builder{
								Key: tc.expected.Type,
								Payload: Ydb.Type_builder{
									VoidType: structpb.NullValue_NULL_VALUE.Enum(),
								}.Build(),
							}.Build(),
						}.Build(),
						Value: Ydb.Value_builder{
							Pairs: []*Ydb.ValuePair{
								Ydb.ValuePair_builder{
									Key: tc.expected.Value,
									Payload: Ydb.Value_builder{
										NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
				}), xtest.ToJSON(params))
		})
	}
}

func TestSet_AddItems(t *testing.T) {
	params := Builder{}.Param("$x").BeginSet().
		AddItems(value.Uint64Value(123), value.Uint64Value(321)).
		EndSet().build().toYDB()
	require.Equal(t, xtest.ToJSON(
		map[string]*Ydb.TypedValue{
			"$x": Ydb.TypedValue_builder{
				Type: Ydb.Type_builder{
					DictType: Ydb.DictType_builder{
						Key: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
						Payload: Ydb.Type_builder{
							VoidType: structpb.NullValue_NULL_VALUE.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Pairs: []*Ydb.ValuePair{
						Ydb.ValuePair_builder{
							Key: Ydb.Value_builder{
								Uint64Value: proto.Uint64(123),
							}.Build(),
							Payload: Ydb.Value_builder{
								NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
							}.Build(),
						}.Build(),
						Ydb.ValuePair_builder{
							Key: Ydb.Value_builder{
								Uint64Value: proto.Uint64(321),
							}.Build(),
							Payload: Ydb.Value_builder{
								NullFlagValue: structpb.NullValue_NULL_VALUE.Enum(),
							}.Build(),
						}.Build(),
					},
				}.Build(),
			}.Build(),
		}), xtest.ToJSON(params))
}
