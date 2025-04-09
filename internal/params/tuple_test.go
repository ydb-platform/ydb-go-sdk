package params

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestTuple(t *testing.T) {
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
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: 123,
					},
				},
			},
		},
		{
			method: "Int64",
			args:   []any{int64(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int64Value{
						Int64Value: 123,
					},
				},
			},
		},
		{
			method: "Uint32",
			args:   []any{uint32(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT32},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 123,
					},
				},
			},
		},
		{
			method: "Int32",
			args:   []any{int32(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int32Value{
						Int32Value: 123,
					},
				},
			},
		},
		{
			method: "Uint16",
			args:   []any{uint16(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT16},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 123,
					},
				},
			},
		},
		{
			method: "Int16",
			args:   []any{int16(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT16},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int32Value{
						Int32Value: 123,
					},
				},
			},
		},
		{
			method: "Uint8",
			args:   []any{uint8(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT8},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 123,
					},
				},
			},
		},
		{
			method: "Int8",
			args:   []any{int8(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT8},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int32Value{
						Int32Value: 123,
					},
				},
			},
		},
		{
			method: "Bool",
			args:   []any{true},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_BoolValue{
						BoolValue: true,
					},
				},
			},
		},
		{
			method: "Text",
			args:   []any{"test"},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: "test",
					},
				},
			},
		},
		{
			method: "Bytes",
			args:   []any{[]byte("test")},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_BytesValue{
						BytesValue: []byte("test"),
					},
				},
			},
		},
		{
			method: "Float",
			args:   []any{float32(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_FloatValue{
						FloatValue: float32(123),
					},
				},
			},
		},
		{
			method: "Double",
			args:   []any{float64(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_DoubleValue{
						DoubleValue: float64(123),
					},
				},
			},
		},
		{
			method: "Interval",
			args:   []any{time.Second},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INTERVAL},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Int64Value{
						Int64Value: 1000000,
					},
				},
			},
		},
		{
			method: "Datetime",
			args:   []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATETIME},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 123456789,
					},
				},
			},
		},
		{
			method: "Date",
			args:   []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint32Value{
						Uint32Value: 1428,
					},
				},
			},
		},
		{
			method: "Timestamp",
			args:   []any{time.Unix(123456789, 456)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Uint64Value{
						Uint64Value: 123456789000000,
					},
				},
			},
		},
		{
			method: "Decimal",
			args:   []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, uint32(22), uint32(9)},

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
				},
			},
		},
		{
			method: "JSON",
			args:   []any{`{"a": 1,"b": "B"}`},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: `{"a": 1,"b": "B"}`,
					},
				},
			},
		},
		{
			method: "JSONDocument",
			args:   []any{`{"a": 1,"b": "B"}`},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON_DOCUMENT},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: `{"a": 1,"b": "B"}`,
					},
				},
			},
		},
		{
			method: "YSON",
			args:   []any{[]byte(`{"a": 1,"b": "B"}`)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_YSON},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_BytesValue{
						BytesValue: []byte(`{"a": 1,"b": "B"}`),
					},
				},
			},
		},
		{
			method: "Uuid",
			args:   []any{uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UUID},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Low_128{
						Low_128: 506660481424032516,
					},
					High_128: 1157159078456920585,
				},
			},
		},
		{
			method: "UUIDWithIssue1501Value",
			args:   []any{[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UUID},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_Low_128{
						Low_128: 651345242494996240,
					},
					High_128: 72623859790382856,
				},
			},
		},
		{
			method: "TzDatetime",
			args:   []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATETIME},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: "1973-11-29T21:33:09,UTC",
					},
				},
			},
		},
		{
			method: "TzDate",
			args:   []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATE},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: "1973-11-29,UTC",
					},
				},
			},
		},
		{
			method: "TzTimestamp",
			args:   []any{time.Unix(123456789, 456).UTC()},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_TIMESTAMP},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{
						TextValue: "1973-11-29T21:33:09.000000,UTC",
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			item := Builder{}.Param("$x").BeginTuple().Add()

			result, ok := xtest.CallMethod(item, tc.method, tc.args...)[0].(*tuple)
			require.True(t, ok)

			params := result.EndTuple().build().toYDB()
			require.Equal(t, xtest.ToJSON(
				map[string]*Ydb.TypedValue{
					"$x": {
						Type: &Ydb.Type{
							Type: &Ydb.Type_TupleType{
								TupleType: &Ydb.TupleType{
									Elements: []*Ydb.Type{
										tc.expected.Type,
									},
								},
							},
						},
						Value: &Ydb.Value{
							Items: []*Ydb.Value{
								tc.expected.Value,
							},
						},
					},
				}), xtest.ToJSON(params))
		})
	}
}

func TestTuple_AddItems(t *testing.T) {
	params := Builder{}.Param("$x").BeginTuple().
		AddItems(value.Uint64Value(123), value.Uint64Value(321)).
		EndTuple().build().toYDB()
	require.Equal(t, xtest.ToJSON(
		map[string]*Ydb.TypedValue{
			"$x": {
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
		}), xtest.ToJSON(params))
}
