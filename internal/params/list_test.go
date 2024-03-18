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

func TestList(t *testing.T) {
	for _, tt := range []struct {
		name    string
		builder Builder
		params  map[string]*Ydb.TypedValue
	}{
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginList().Add().Uint64(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Int64(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Uint32(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Int32(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Uint16(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Int16(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Uint8(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Int8(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Bool(true).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Text("test").EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Bytes([]byte("test")).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Float(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Double(123).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Interval(time.Second).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Datetime(time.Unix(123456789, 456)).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Date(time.Unix(123456789, 456)).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Timestamp(time.Unix(123456789, 456)).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().Decimal([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9).EndList(), //nolint:lll
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
			builder: Builder{}.Param("$x").BeginList().Add().JSON(`{"a": 1,"b": "B"}`).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().JSONDocument(`{"a": 1,"b": "B"}`).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().YSON([]byte(`[ 1; 2; 3; 4; 5 ]`)).EndList(),
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
			builder: Builder{}.Param("$x").BeginList().Add().
				UUID([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).EndList(),
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
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginList().AddItems(value.Uint64Value(123), value.Uint64Value(321)).EndList(),
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
			require.Equal(t, paramsToJSON(tt.params), paramsToJSON(params))
		})
	}
}
