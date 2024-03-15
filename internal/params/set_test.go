package params

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestSet(t *testing.T) {
	for _, tt := range []struct {
		name    string
		builder Builder
		params  map[string]*Ydb.TypedValue
	}{
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Uint64(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT64,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint64Value{
										Uint64Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Int64(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT64,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Int64Value{
										Int64Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Uint32(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT32,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint32Value{
										Uint32Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Int32(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT32,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Int32Value{
										Int32Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Uint16(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT16,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint32Value{
										Uint32Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Int16(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT16,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Int32Value{
										Int32Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Uint8(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT8,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint32Value{
										Uint32Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Int8(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT8,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Int32Value{
										Int32Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Bool(true).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_BOOL,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_BoolValue{
										BoolValue: true,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Text("test").EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UTF8,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_TextValue{
										TextValue: "test",
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Bytes([]byte("test")).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_STRING,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_BytesValue{
										BytesValue: []byte("test"),
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Float(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_FLOAT,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_FloatValue{
										FloatValue: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Double(123).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DOUBLE,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_DoubleValue{
										DoubleValue: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Interval(time.Second).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INTERVAL,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Int64Value{
										Int64Value: 1000000,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Datetime(time.Unix(123456789, 456)).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DATETIME,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint32Value{
										Uint32Value: 123456789,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Date(time.Unix(123456789, 456)).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DATE,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint32Value{
										Uint32Value: 1428,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Timestamp(time.Unix(123456789, 456)).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_TIMESTAMP,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint64Value{
										Uint64Value: 123456789000000,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().Decimal([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9).EndSet(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_DecimalType{
										DecimalType: &Ydb.DecimalType{
											Precision: 22,
											Scale:     9,
										},
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									High_128: 72623859790382856,
									Value: &Ydb.Value_Low_128{
										Low_128: 648519454493508870,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().JSON(`{"a": 1,"b": "B"}`).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_JSON,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_TextValue{
										TextValue: `{"a": 1,"b": "B"}`,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().JSONDocument(`{"a": 1,"b": "B"}`).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_JSON_DOCUMENT,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_TextValue{
										TextValue: `{"a": 1,"b": "B"}`,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().YSON([]byte(`[ 1; 2; 3; 4; 5 ]`)).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_YSON,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_BytesValue{
										BytesValue: []byte(`[ 1; 2; 3; 4; 5 ]`),
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name: xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().
				UUID([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).
				EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UUID,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Low_128{
										Low_128: 651345242494996240,
									},
									High_128: 72623859790382856,
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().AddItems(value.Uint64Value(123), value.Uint64Value(321)).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT64,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint64Value{
										Uint64Value: 123,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_Uint64Value{
										Uint64Value: 321,
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().TzDatetime(time.Unix(123456789, 456).UTC()).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_TZ_DATETIME,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_TextValue{
										TextValue: "1973-11-29T21:33:09Z",
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().TzDate(time.Unix(123456789, 456).UTC()).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_TZ_DATE,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_TextValue{
										TextValue: "1973-11-29",
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginSet().Add().TzTimestamp(time.Unix(123456789, 456).UTC()).EndSet(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_DictType{
							DictType: &Ydb.DictType{
								Key: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_TZ_TIMESTAMP,
									},
								},
								Payload: &Ydb.Type{
									Type: &Ydb.Type_VoidType{},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Pairs: []*Ydb.ValuePair{
							{
								Key: &Ydb.Value{
									Value: &Ydb.Value_TextValue{
										TextValue: "1973-11-29T21:33:09.000000Z",
									},
								},
								Payload: &Ydb.Value{
									Value: &Ydb.Value_NullFlagValue{},
								},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := allocator.New()
			defer a.Free()
			params := tt.builder.Build().ToYDB(a)
			require.Equal(t, paramsToJSON(tt.params), paramsToJSON(params))
		})
	}
}
