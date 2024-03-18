package params

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestOptional(t *testing.T) {
	for _, tt := range []struct {
		name    string
		builder Builder
		params  map[string]*Ydb.TypedValue
	}{
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Uint64(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
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
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Int64(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT64,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Int64Value{
							Int64Value: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Uint32(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT32,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Int32(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT32,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Int32Value{
							Int32Value: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Uint16(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT16,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Int16(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT16,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Int32Value{
							Int32Value: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Uint8(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT8,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Int8(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT8,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Int32Value{
							Int32Value: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Bool(true).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_BOOL,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_BoolValue{
							BoolValue: true,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Text("test").EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UTF8,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_TextValue{
							TextValue: "test",
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Bytes([]byte("test")).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_STRING,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_BytesValue{
							BytesValue: []byte("test"),
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Float(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_FLOAT,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_FloatValue{
							FloatValue: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Double(123).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DOUBLE,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_DoubleValue{
							DoubleValue: 123,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Interval(time.Second).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INTERVAL,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Int64Value{
							Int64Value: 1000000,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Datetime(time.Unix(123456789, 456)).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DATETIME,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 123456789,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Date(time.Unix(123456789, 456)).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DATE,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Uint32Value{
							Uint32Value: 1428,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Timestamp(time.Unix(123456789, 456)).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_TIMESTAMP,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 123456789000000,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().Decimal([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9).EndOptional(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
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
					Value: &Ydb.Value{
						High_128: 72623859790382856,
						Value: &Ydb.Value_Low_128{
							Low_128: 648519454493508870,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().JSON(`{"a": 1,"b": "B"}`).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_JSON,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_TextValue{
							TextValue: `{"a": 1,"b": "B"}`,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().JSONDocument(`{"a": 1,"b": "B"}`).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_JSON_DOCUMENT,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_TextValue{
							TextValue: `{"a": 1,"b": "B"}`,
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginOptional().YSON([]byte(`[ 1; 2; 3; 4; 5 ]`)).EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_YSON,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_BytesValue{
							BytesValue: []byte(`[ 1; 2; 3; 4; 5 ]`),
						},
					},
				},
			},
		},
		{
			name: xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").
				BeginOptional().
				UUID([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).
				EndOptional(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_OptionalType{
							OptionalType: &Ydb.OptionalType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UUID,
									},
								},
							},
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_Low_128{
							Low_128: 651345242494996240,
						},
						High_128: 72623859790382856,
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
