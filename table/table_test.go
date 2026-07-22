package table_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Formats"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
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
			request: Ydb_Table.BulkUpsertRequest_builder{
				Table: "test",
				Rows: Ydb.TypedValue_builder{
					Type: Ydb.Type_builder{
						ListType: Ydb.ListType_builder{
							Item: Ydb.Type_builder{TypeId: Ydb.Type_UINT64.Enum()}.Build(),
						}.Build(),
					}.Build(),
					Value: Ydb.Value_builder{
						Items: []*Ydb.Value{
							Ydb.Value_builder{
								Uint64Value: proto.Uint64(123),
							}.Build(),
							Ydb.Value_builder{
								Uint64Value: proto.Uint64(321),
							}.Build(),
						},
					}.Build(),
				}.Build(),
			}.Build(),
		},
		{
			name: "Csv",
			data: table.BulkUpsertDataCsv([]byte("123")),
			request: Ydb_Table.BulkUpsertRequest_builder{
				Table:       "test",
				Data:        []byte("123"),
				CsvSettings: &Ydb_Formats.CsvSettings{},
			}.Build(),
		},
		{
			name: "CsvWithDelimeter",
			data: table.BulkUpsertDataCsv([]byte("123"), table.WithCsvDelimiter([]byte(";"))),
			request: Ydb_Table.BulkUpsertRequest_builder{
				Table: "test",
				Data:  []byte("123"),
				CsvSettings: Ydb_Formats.CsvSettings_builder{
					Delimiter: []byte(";"),
				}.Build(),
			}.Build(),
		},
		{
			name: "CsvWithHeader",
			data: table.BulkUpsertDataCsv([]byte("123"), table.WithCsvHeader()),
			request: Ydb_Table.BulkUpsertRequest_builder{
				Table: "test",
				Data:  []byte("123"),
				CsvSettings: Ydb_Formats.CsvSettings_builder{
					Header: true,
				}.Build(),
			}.Build(),
		},
		{
			name: "CsvWithNullValue",
			data: table.BulkUpsertDataCsv([]byte("123"), table.WithCsvNullValue([]byte("null"))),
			request: Ydb_Table.BulkUpsertRequest_builder{
				Table: "test",
				Data:  []byte("123"),
				CsvSettings: Ydb_Formats.CsvSettings_builder{
					NullValue: []byte("null"),
				}.Build(),
			}.Build(),
		},
		{
			name: "CsvWithNullValue",
			data: table.BulkUpsertDataCsv([]byte("123"), table.WithCsvSkipRows(30)),
			request: Ydb_Table.BulkUpsertRequest_builder{
				Table: "test",
				Data:  []byte("123"),
				CsvSettings: Ydb_Formats.CsvSettings_builder{
					SkipRows: 30,
				}.Build(),
			}.Build(),
		},
		{
			name: "Arrow",
			data: table.BulkUpsertDataArrow([]byte("123")),
			request: Ydb_Table.BulkUpsertRequest_builder{
				Table:              "test",
				Data:               []byte("123"),
				ArrowBatchSettings: &Ydb_Formats.ArrowBatchSettings{},
			}.Build(),
		},
		{
			name: "ArrowWithSchema",
			data: table.BulkUpsertDataArrow([]byte("123"),
				table.WithArrowSchema([]byte("schema")),
			),
			request: Ydb_Table.BulkUpsertRequest_builder{
				Table: "test",
				Data:  []byte("123"),
				ArrowBatchSettings: Ydb_Formats.ArrowBatchSettings_builder{
					Schema: []byte("schema"),
				}.Build(),
			}.Build(),
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
