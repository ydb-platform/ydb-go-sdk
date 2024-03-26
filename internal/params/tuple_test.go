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

func TestTuple(t *testing.T) {
	for _, tt := range []struct {
		name    string
		builder Builder
		params  map[string]*Ydb.TypedValue
	}{
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Uint64(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
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
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Int64(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_INT64,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Int64Value{
									Int64Value: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Uint32(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT32,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Uint32Value{
									Uint32Value: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Int32(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_INT32,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Int32Value{
									Int32Value: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Uint16(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT16,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Uint32Value{
									Uint32Value: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Int16(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_INT16,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Int32Value{
									Int32Value: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Uint8(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT8,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Uint32Value{
									Uint32Value: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Int8(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_INT8,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Int32Value{
									Int32Value: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Bool(true).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_BOOL,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_BoolValue{
									BoolValue: true,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Text("test").EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UTF8,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_TextValue{
									TextValue: "test",
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Bytes([]byte("test")).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_STRING,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_BytesValue{
									BytesValue: []byte("test"),
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Float(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_FLOAT,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_FloatValue{
									FloatValue: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Double(123).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_DOUBLE,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_DoubleValue{
									DoubleValue: 123,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Interval(time.Second).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_INTERVAL,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Int64Value{
									Int64Value: 1000000,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Datetime(time.Unix(123456789, 456)).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_DATETIME,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Uint32Value{
									Uint32Value: 123456789,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Date(time.Unix(123456789, 456)).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_DATE,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Uint32Value{
									Uint32Value: 1428,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Timestamp(time.Unix(123456789, 456)).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_TIMESTAMP,
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
									Uint64Value: 123456789000000,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().Decimal([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9).EndTuple(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_DecimalType{
											DecimalType: &Ydb.DecimalType{
												Precision: 22,
												Scale:     9,
											},
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								High_128: 72623859790382856,
								Value: &Ydb.Value_Low_128{
									Low_128: 648519454493508870,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().JSON(`{"a": 1,"b": "B"}`).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_JSON,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_TextValue{
									TextValue: `{"a": 1,"b": "B"}`,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().JSONDocument(`{"a": 1,"b": "B"}`).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_JSON_DOCUMENT,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_TextValue{
									TextValue: `{"a": 1,"b": "B"}`,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().YSON([]byte(`[ 1; 2; 3; 4; 5 ]`)).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_YSON,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_BytesValue{
									BytesValue: []byte(`[ 1; 2; 3; 4; 5 ]`),
								},
							},
						},
					},
				},
			},
		},
		{
			name: xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().Add().
				UUID([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).EndTuple(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TupleType{
							TupleType: &Ydb.TupleType{
								Elements: []*Ydb.Type{
									{
										Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UUID,
										},
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Low_128{
									Low_128: 651345242494996240,
								},
								High_128: 72623859790382856,
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginTuple().AddItems(value.Uint64Value(123), value.Uint64Value(321)).EndTuple(),
			params: map[string]*Ydb.TypedValue{
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
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := allocator.New()
			defer a.Free()
			params := tt.builder.Build().ToYDB(a)
			require.Equal(t, xtest.ToJSON(tt.params), xtest.ToJSON(params))
		})
	}
}
