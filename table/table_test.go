package table_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Formats"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestQueryParameters_String(t *testing.T) {
	for _, tt := range []struct {
		p *table.QueryParameters
		s string
	}{
		{
			p: nil,
			s: `{}`,
		},
		{
			p: table.NewQueryParameters(),
			s: `{}`,
		},
		{
			p: table.NewQueryParameters(
				table.ValueParam("$a", types.TextValue("test")),
			),
			s: `{"$a":"test"u}`,
		},
		{
			p: table.NewQueryParameters(
				table.ValueParam("$a", types.TextValue("test")),
				table.ValueParam("$b", types.BytesValue([]byte("test"))),
			),
			s: `{"$a":"test"u,"$b":"test"}`,
		},
		{
			p: table.NewQueryParameters(
				table.ValueParam("$a", types.TextValue("test")),
				table.ValueParam("$b", types.BytesValue([]byte("test"))),
				table.ValueParam("$c", types.Uint64Value(123456)),
			),
			s: `{"$a":"test"u,"$b":"test","$c":123456ul}`,
		},
		{
			p: table.NewQueryParameters(
				table.ValueParam("$a", types.TextValue("test")),
				table.ValueParam("$b", types.BytesValue([]byte("test"))),
				table.ValueParam("$c", types.Uint64Value(123456)),
				table.ValueParam("$d", types.StructValue(
					types.StructFieldValue("$a", types.TextValue("test")),
					types.StructFieldValue("$b", types.BytesValue([]byte("test"))),
					types.StructFieldValue("$c", types.Uint64Value(123456)),
				)),
			),
			s: "{\"$a\":\"test\"u,\"$b\":\"test\",\"$c\":123456ul,\"$d\":<|`$a`:\"test\"u,`$b`:\"test\",`$c`:123456ul|>}",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.s, tt.p.String())
		})
	}
}

func TestBulkUpsertData(t *testing.T) {
	for _, tt := range []struct {
		name    string
		data    table.BulkUpsertData
		request *Ydb_Table.BulkUpsertRequest
	}{
		{
			name: "Rows",
			data: table.BulkUpsertDataRows(types.ListValue(
				types.Uint64Value(123),
				types.Uint64Value(321),
			)),
			request: &Ydb_Table.BulkUpsertRequest{
				Table: "test",
				Rows: &Ydb.TypedValue{
					Type: &Ydb.Type{
						Type: &Ydb.Type_ListType{
							ListType: &Ydb.ListType{
								Item: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
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
		{
			name: "Csv",
			data: table.BulkUpsertDataCsv([]byte("123")),
			request: &Ydb_Table.BulkUpsertRequest{
				Table: "test",
				Data:  []byte("123"),
				DataFormat: &Ydb_Table.BulkUpsertRequest_CsvSettings{
					CsvSettings: &Ydb_Formats.CsvSettings{},
				},
			},
		},
		{
			name: "CsvWithDelimeter",
			data: table.BulkUpsertDataCsv([]byte("123"), table.WithCsvDelimiter([]byte(";"))),
			request: &Ydb_Table.BulkUpsertRequest{
				Table: "test",
				Data:  []byte("123"),
				DataFormat: &Ydb_Table.BulkUpsertRequest_CsvSettings{
					CsvSettings: &Ydb_Formats.CsvSettings{
						Delimiter: []byte(";"),
					},
				},
			},
		},
		{
			name: "CsvWithHeader",
			data: table.BulkUpsertDataCsv([]byte("123"), table.WithCsvHeader()),
			request: &Ydb_Table.BulkUpsertRequest{
				Table: "test",
				Data:  []byte("123"),
				DataFormat: &Ydb_Table.BulkUpsertRequest_CsvSettings{
					CsvSettings: &Ydb_Formats.CsvSettings{
						Header: true,
					},
				},
			},
		},
		{
			name: "CsvWithNullValue",
			data: table.BulkUpsertDataCsv([]byte("123"), table.WithCsvNullValue([]byte("null"))),
			request: &Ydb_Table.BulkUpsertRequest{
				Table: "test",
				Data:  []byte("123"),
				DataFormat: &Ydb_Table.BulkUpsertRequest_CsvSettings{
					CsvSettings: &Ydb_Formats.CsvSettings{
						NullValue: []byte("null"),
					},
				},
			},
		},
		{
			name: "CsvWithNullValue",
			data: table.BulkUpsertDataCsv([]byte("123"), table.WithCsvSkipRows(30)),
			request: &Ydb_Table.BulkUpsertRequest{
				Table: "test",
				Data:  []byte("123"),
				DataFormat: &Ydb_Table.BulkUpsertRequest_CsvSettings{
					CsvSettings: &Ydb_Formats.CsvSettings{
						SkipRows: 30,
					},
				},
			},
		},
		{
			name: "Arrow",
			data: table.BulkUpsertDataArrow([]byte("123")),
			request: &Ydb_Table.BulkUpsertRequest{
				Table: "test",
				Data:  []byte("123"),
				DataFormat: &Ydb_Table.BulkUpsertRequest_ArrowBatchSettings{
					ArrowBatchSettings: &Ydb_Formats.ArrowBatchSettings{},
				},
			},
		},
		{
			name: "ArrowWithSchema",
			data: table.BulkUpsertDataArrow([]byte("123"),
				table.WithArrowSchema([]byte("schema")),
			),
			request: &Ydb_Table.BulkUpsertRequest{
				Table: "test",
				Data:  []byte("123"),
				DataFormat: &Ydb_Table.BulkUpsertRequest_ArrowBatchSettings{
					ArrowBatchSettings: &Ydb_Formats.ArrowBatchSettings{
						Schema: []byte("schema"),
					},
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request, err := tt.data.ToYDB("test")
			require.NoError(t, err)
			require.Equal(t,
				tt.request.String(),
				request.String(),
			)
		})
	}
}
