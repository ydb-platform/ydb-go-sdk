package params

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestOptional(t *testing.T) {
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
			args:   []any{p(uint64(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint64Value: proto.Uint64(123),
				}.Build(),
			},
		},
		{
			method: "Int64",
			args:   []any{p(int64(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_INT64.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value: proto.Int64(123),
				}.Build(),
			},
		},
		{
			method: "Uint32",
			args:   []any{p(uint32(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT32.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(123),
				}.Build(),
			},
		},
		{
			method: "Int32",
			args:   []any{p(int32(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_INT32.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value: proto.Int32(123),
				}.Build(),
			},
		},
		{
			method: "Uint16",
			args:   []any{p(uint16(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT16.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(123),
				}.Build(),
			},
		},
		{
			method: "Int16",
			args:   []any{p(int16(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_INT16.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value: proto.Int32(123),
				}.Build(),
			},
		},
		{
			method: "Uint8",
			args:   []any{p(uint8(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT8.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(123),
				}.Build(),
			},
		},
		{
			method: "Int8",
			args:   []any{p(int8(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_INT8.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value: proto.Int32(123),
				}.Build(),
			},
		},
		{
			method: "Bool",
			args:   []any{p(true)},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_BOOL.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					BoolValue: proto.Bool(true),
				}.Build(),
			},
		},
		{
			method: "Text",
			args:   []any{p("test")},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("test"),
				}.Build(),
			},
		},
		{
			method: "Bytes",
			args:   []any{p([]byte("test"))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_STRING.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					BytesValue: []byte("test"),
				}.Build(),
			},
		},
		{
			method: "Float",
			args:   []any{p(float32(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_FLOAT.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					FloatValue: proto.Float32(float32(123)),
				}.Build(),
			},
		},
		{
			method: "Double",
			args:   []any{p(float64(123))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_DOUBLE.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					DoubleValue: proto.Float64(float64(123)),
				}.Build(),
			},
		},
		{
			method: "Interval",
			args:   []any{p(time.Second)},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_INTERVAL.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value: proto.Int64(1000000),
				}.Build(),
			},
		},
		{
			method: "Interval64",
			args:   []any{p(123456789 * time.Microsecond)},
			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_INTERVAL64.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value: proto.Int64(123456789), // nanoseconds
				}.Build(),
			},
		},
		{
			method: "Datetime",
			args:   []any{p(time.Unix(123456789, 456))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_DATETIME.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(123456789),
				}.Build(),
			},
		},
		{
			method: "Datetime64",
			args:   []any{p(time.Unix(123456789, 456))},
			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_DATETIME64.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value: proto.Int64(123456789),
				}.Build(),
			},
		},
		{
			method: "Date",
			args:   []any{p(time.Unix(123456789, 456))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_DATE.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint32Value: proto.Uint32(1428),
				}.Build(),
			},
		},
		{
			method: "Date32",
			args:   []any{p(time.Unix(123456789, 456))},
			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_DATE32.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int32Value: proto.Int32(1428),
				}.Build(),
			},
		},
		{
			method: "Timestamp",
			args:   []any{p(time.Unix(123456789, 456))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_TIMESTAMP.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Uint64Value: proto.Uint64(123456789000000),
				}.Build(),
			},
		},
		{
			method: "Timestamp64",
			args:   []any{p(time.Unix(123456789, 123456000))},
			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_TIMESTAMP64.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Int64Value: proto.Int64(123456789123456), // microseconds
				}.Build(),
			},
		},
		{
			method: "Decimal",
			args:   []any{p([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}), uint32(22), uint32(9)},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							DecimalType: Ydb.DecimalType_builder{
								Precision: 22,
								Scale:     9,
							}.Build(),
						}.Build(),
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
			args:   []any{p(`{"a": 1,"b": "B"}`)},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_JSON.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String(`{"a": 1,"b": "B"}`),
				}.Build(),
			},
		},
		{
			method: "JSONDocument",
			args:   []any{p(`{"a": 1,"b": "B"}`)},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_JSON_DOCUMENT.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String(`{"a": 1,"b": "B"}`),
				}.Build(),
			},
		},
		{
			method: "YSON",
			args:   []any{p([]byte(`{"a": 1,"b": "B"}`))},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_YSON.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					BytesValue: []byte(`{"a": 1,"b": "B"}`),
				}.Build(),
			},
		},
		{
			method: "Uuid",
			args:   []any{p(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_UUID.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Low_128:  proto.Uint64(506660481424032516),
					High_128: 1157159078456920585,
				}.Build(),
			},
		},
		{
			method: "UUIDWithIssue1501Value",
			args:   []any{p([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_UUID.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					Low_128:  proto.Uint64(651345242494996240),
					High_128: 72623859790382856,
				}.Build(),
			},
		},
		{
			method: "TzDatetime",
			args:   []any{p(time.Unix(123456789, 456).UTC())},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_TZ_DATETIME.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("1973-11-29T21:33:09,UTC"),
				}.Build(),
			},
		},
		{
			method: "TzDate",
			args:   []any{p(time.Unix(123456789, 456).UTC())},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_TZ_DATE.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("1973-11-29,UTC"),
				}.Build(),
			},
		},
		{
			method: "TzTimestamp",
			args:   []any{p(time.Unix(123456789, 456).UTC())},

			expected: expected{
				Type: Ydb.Type_builder{
					OptionalType: Ydb.OptionalType_builder{
						Item: Ydb.Type_builder{
							TypeId: Ydb.Type_TZ_TIMESTAMP.Enum(),
						}.Build(),
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("1973-11-29T21:33:09.000000,UTC"),
				}.Build(),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			item := Builder{}.Param("$x").BeginOptional()

			result, ok := xtest.CallMethod(item, tc.method, tc.args...)[0].(*optionalBuilder)
			require.True(t, ok)

			params := result.EndOptional().build().toYDB()
			require.Equal(t, xtest.ToJSON(
				map[string]*Ydb.TypedValue{
					"$x": Ydb.TypedValue_builder{
						Type:  tc.expected.Type,
						Value: tc.expected.Value,
					}.Build(),
				}), xtest.ToJSON(params))
		})
	}
}

func p[T any](v T) *T {
	return &v
}
