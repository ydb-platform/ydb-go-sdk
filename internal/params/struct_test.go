package params

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestStruct(t *testing.T) {
	for _, tt := range []struct {
		name    string
		builder Builder
		params  map[string]*Ydb.TypedValue
	}{
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Uint64(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UINT64,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Int64(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_INT64,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Uint32(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UINT32,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Int32(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_INT32,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Uint16(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UINT16,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Int16(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_INT16,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Uint8(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UINT8,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Int8(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_INT8,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Bool(true).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_BOOL,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Text("test").EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UTF8,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Bytes([]byte("test")).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_STRING,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Float(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_FLOAT,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Double(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_DOUBLE,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Interval(time.Second).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_INTERVAL,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Datetime(time.Unix(123456789, 456)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_DATETIME,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Date(time.Unix(123456789, 456)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_DATE,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Timestamp(time.Unix(123456789, 456)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_TIMESTAMP,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Decimal([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9).EndStruct(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").JSON(`{"a": 1,"b": "B"}`).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_JSON,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").JSONDocument(`{"a": 1,"b": "B"}`).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_JSON_DOCUMENT,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").YSON([]byte(`[ 1; 2; 3; 4; 5 ]`)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_YSON,
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
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").
				UUIDWithIssue1501Value([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UUID,
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
			name: xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").
				Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UUID,
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
								Value: &Ydb.Value_Low_128{
									Low_128: 506660481424032516,
								},
								High_128: 1157159078456920585,
							},
						},
					},
				},
			},
		},
		{
			name: xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().
				Field("col1").Text("text").
				Field("col2").Uint32(123).
				Field("col3").Int64(456).
				EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UTF8,
											},
										},
									},
									{
										Name: "col2",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_UINT32,
											},
										},
									},
									{
										Name: "col3",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_INT64,
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
								Value: &Ydb.Value_TextValue{
									TextValue: "text",
								},
							},
							{
								Value: &Ydb.Value_Uint32Value{
									Uint32Value: 123,
								},
							},
							{
								Value: &Ydb.Value_Int64Value{
									Int64Value: 456,
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").TzDatetime(time.Unix(123456789, 456).UTC()).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_TZ_DATETIME,
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
								Value: &Ydb.Value_TextValue{
									TextValue: "1973-11-29T21:33:09,UTC",
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").TzDate(time.Unix(123456789, 456).UTC()).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_TZ_DATE,
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
								Value: &Ydb.Value_TextValue{
									TextValue: "1973-11-29,UTC",
								},
							},
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").TzTimestamp(time.Unix(123456789, 456).UTC()).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_StructType{
							StructType: &Ydb.StructType{
								Members: []*Ydb.StructMember{
									{
										Name: "col1",
										Type: &Ydb.Type{
											Type: &Ydb.Type_TypeId{
												TypeId: Ydb.Type_TZ_TIMESTAMP,
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
								Value: &Ydb.Value_TextValue{
									TextValue: "1973-11-29T21:33:09.000000,UTC",
								},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := tt.builder.build().toYDB()
			require.Equal(t, xtest.ToJSON(tt.params), xtest.ToJSON(params))
		})
	}
}
