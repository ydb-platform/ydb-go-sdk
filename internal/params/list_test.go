package params

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestList(t *testing.T) {
	for _, tt := range []struct {
		name    string
		builder Builder
		params  map[string]*Ydb.TypedValue
	}{
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").List().AddItem().Uint64(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
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
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").List().AddItem().Int64(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT64,
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
			builder: Builder{}.Param("$x").List().AddItem().Uint32(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT32,
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
			builder: Builder{}.Param("$x").List().AddItem().Int32(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT32,
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
			builder: Builder{}.Param("$x").List().AddItem().Uint16(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT16,
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
			builder: Builder{}.Param("$x").List().AddItem().Int16(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT16,
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
			builder: Builder{}.Param("$x").List().AddItem().Uint8(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UINT8,
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
			builder: Builder{}.Param("$x").List().AddItem().Int8(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INT8,
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
			builder: Builder{}.Param("$x").List().AddItem().Bool(true).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_BOOL,
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
			builder: Builder{}.Param("$x").List().AddItem().Text("test").Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UTF8,
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
			builder: Builder{}.Param("$x").List().AddItem().Bytes([]byte("test")).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_STRING,
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
			builder: Builder{}.Param("$x").List().AddItem().Float(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_FLOAT,
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
			builder: Builder{}.Param("$x").List().AddItem().Double(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DOUBLE,
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
			builder: Builder{}.Param("$x").List().AddItem().Interval(time.Second).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_INTERVAL,
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
			builder: Builder{}.Param("$x").List().AddItem().Datetime(time.Unix(123456789, 456)).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DATETIME,
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
			builder: Builder{}.Param("$x").List().AddItem().Date(time.Unix(123456789, 456)).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_DATE,
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
			builder: Builder{}.Param("$x").List().AddItem().Timestamp(time.Unix(123456789, 456)).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_TIMESTAMP,
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
			builder: Builder{}.Param("$x").List().AddItem().Decimal([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9).Build(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
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
			builder: Builder{}.Param("$x").List().AddItem().JSON(`{"a": 1,"b": "B"}`).Build(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_JSON,
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
			builder: Builder{}.Param("$x").List().AddItem().JSONDocument(`{"a": 1,"b": "B"}`).Build(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_JSON_DOCUMENT,
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
			builder: Builder{}.Param("$x").List().AddItem().YSON([]byte(`[ 1; 2; 3; 4; 5 ]`)).Build(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_YSON,
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
			builder: Builder{}.Param("$x").List().AddItem().
				UUID([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).Build(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{
									Type: &Ydb.Type_TypeId{
										TypeId: Ydb.Type_UUID,
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
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := allocator.New()
			defer a.Free()
			params := tt.builder.Build().ToYDB(a)
			require.Equal(t, paramsToJSON(tt.params), paramsToJSON(params))
		})
	}
}
