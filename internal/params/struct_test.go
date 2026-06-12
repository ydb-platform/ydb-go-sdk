package params

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"google.golang.org/protobuf/proto"
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
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UINT64.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Uint64Value: proto.Uint64(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Int64(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_INT64.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Int64Value: proto.Int64(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Uint32(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UINT32.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Uint32Value: proto.Uint32(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Int32(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_INT32.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Int32Value: proto.Int32(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Uint16(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UINT16.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Uint32Value: proto.Uint32(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Int16(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_INT16.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Int32Value: proto.Int32(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Uint8(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UINT8.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Uint32Value: proto.Uint32(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Int8(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_INT8.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Int32Value: proto.Int32(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Bool(true).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_BOOL.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Text("test").EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UTF8.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String("test"),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Bytes([]byte("test")).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_STRING.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								BytesValue: []byte("test"),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Float(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_FLOAT.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								FloatValue: proto.Float32(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Double(123).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_DOUBLE.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								DoubleValue: proto.Float64(123),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Interval(time.Second).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_INTERVAL.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Int64Value: proto.Int64(1000000),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Datetime(time.Unix(123456789, 456)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_DATETIME.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Uint32Value: proto.Uint32(123456789),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Date(time.Unix(123456789, 456)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_DATE.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Uint32Value: proto.Uint32(1428),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Timestamp(time.Unix(123456789, 456)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_TIMESTAMP.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Uint64Value: proto.Uint64(123456789000000),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").Decimal([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9).EndStruct(), //nolint:lll
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										DecimalType: Ydb.DecimalType_builder{
											Precision: 22,
											Scale:     9,
										}.Build(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								High_128: 72623859790382856,
								Low_128:  proto.Uint64(648519454493508870),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").JSON(`{"a": 1,"b": "B"}`).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_JSON.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String(`{"a": 1,"b": "B"}`),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").JSONFromBytes([]byte(`{"a": 1,"b": "B"}`)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_JSON.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String(`{"a": 1,"b": "B"}`),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").JSONDocument(`{"a": 1,"b": "B"}`).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_JSON_DOCUMENT.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String(`{"a": 1,"b": "B"}`),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name: xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").
				BeginStruct().
				Field("col1").
				JSONDocumentFromBytes([]byte(`{"a": 1,"b": "B"}`)).
				EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_JSON_DOCUMENT.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String(`{"a": 1,"b": "B"}`),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").YSON([]byte(`[ 1; 2; 3; 4; 5 ]`)).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_YSON.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								BytesValue: []byte(`[ 1; 2; 3; 4; 5 ]`),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name: xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").
				UUIDWithIssue1501Value([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UUID.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Low_128:  proto.Uint64(651345242494996240),
								High_128: 72623859790382856,
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name: xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").
				Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UUID.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Low_128:  proto.Uint64(506660481424032516),
								High_128: 1157159078456920585,
							}.Build(),
						},
					}.Build(),
				}.Build(),
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
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UTF8.Enum(),
									}.Build(),
								}.Build(),
								Ydb.StructMember_builder{
									Name: "col2",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_UINT32.Enum(),
									}.Build(),
								}.Build(),
								Ydb.StructMember_builder{
									Name: "col3",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_INT64.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String("text"),
							}.Build(),
							Ydb.Value_builder{
								Uint32Value: proto.Uint32(123),
							}.Build(),
							Ydb.Value_builder{
								Int64Value: proto.Int64(456),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").TzDatetime(time.Unix(123456789, 456).UTC()).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_TZ_DATETIME.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String("1973-11-29T21:33:09,UTC"),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").TzDate(time.Unix(123456789, 456).UTC()).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_TZ_DATE.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String("1973-11-29,UTC"),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Builder{}.Param("$x").BeginStruct().Field("col1").TzTimestamp(time.Unix(123456789, 456).UTC()).EndStruct(),
			params: map[string]*Ydb.TypedValue{
				"$x": Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						StructType: Ydb.StructType_builder{
							Members: []*Ydb.StructMember{
								Ydb.StructMember_builder{
									Name: "col1",
									Type: Ydb.Type_builder{
										TypeId: Ydb.Type_TZ_TIMESTAMP.Enum(),
									}.Build(),
								}.Build(),
							},
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								TextValue: proto.String("1973-11-29T21:33:09.000000,UTC"),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := tt.builder.build().toYDB()
			require.Equal(t, xtest.ToJSON(tt.params), xtest.ToJSON(params))
		})
	}
}
