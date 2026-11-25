package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wirevalue"
)

func TestQueryWireValuesReportsInvalidVariant(t *testing.T) {
	variant := types.NewVariantTuple(types.Int32)
	server := &optionalWireResultServer{
		columns: []*Ydb.Column{{Name: "choice", Type: types.TypeToYDB(variant)}},
		rows: []*Ydb.Value{{Items: []*Ydb.Value{{
			VariantIndex: 1,
			Value: &Ydb.Value_NestedValue{NestedValue: &Ydb.Value{
				Value: &Ydb.Value_Int32Value{Int32Value: 42},
			}},
		}}}},
	}
	conn := newWireResultConn(t, server)
	ctx := t.Context()
	result, err := execute(ctx, "session", WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(conn)),
		"SELECT choice", options.ExecuteSettings(), options.ResultSetsTypeOrdered)
	require.NoError(t, err)
	defer func() { require.NoError(t, result.Close(ctx)) }()
	set, err := result.NextResultSet(ctx)
	require.NoError(t, err)
	row, err := set.NextRow(ctx)
	require.NoError(t, err)
	require.PanicsWithError(t, "scan error on column index 0: variant tuple index 1 out of range", func() {
		row.Values()
	})
}

func TestWireValuesFailAtTheMalformedColumn(t *testing.T) {
	malformedCell := protowire.AppendTag(nil, wirevalue.ValueItemsField, wirevalue.ValueItemsWireType)
	malformedCell = protowire.AppendBytes(malformedCell, []byte{0xff})
	for _, tt := range []struct {
		name  string
		frame []byte
		want  string
	}{
		{name: "missing cell", want: "wire value decoder: missing column 0"},
		{name: "malformed row", frame: []byte{0xff}, want: "unexpected EOF"},
		{name: "malformed cell", frame: malformedCell, want: "unexpected EOF"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			part := &wirePart{frame: tt.frame, rows: []rowSpan{{end: uint32(len(tt.frame))}}}
			row := part.row(0, []*Ydb.Column{{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)}})
			require.PanicsWithError(t, "scan error on column index 0: "+tt.want, func() { row.Values() })
		})
	}
}

func TestWireValuesFailureDoesNotInvalidateOtherCells(t *testing.T) {
	good, err := proto.Marshal(&Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: 42}})
	require.NoError(t, err)
	var frame []byte
	for _, cell := range [][]byte{{0xff}, good} {
		frame = protowire.AppendTag(frame, wirevalue.ValueItemsField, wirevalue.ValueItemsWireType)
		frame = protowire.AppendBytes(frame, cell)
	}
	part := &wirePart{frame: frame, rows: []rowSpan{{end: uint32(len(frame))}}}
	row := part.row(0, []*Ydb.Column{
		{Name: "bad", Type: wirePrimitive(Ydb.Type_UINT64)},
		{Name: "good", Type: wirePrimitive(Ydb.Type_UINT64)},
	})
	require.PanicsWithError(t, "scan error on column index 0: unexpected EOF", func() { row.Values() })
	var got uint64
	require.NoError(t, row.ScanNamed(scanner.NamedRef("good", &got)))
	require.Equal(t, uint64(42), got)
}
