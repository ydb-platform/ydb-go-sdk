package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestDecodeWireValuePartScansBenchmarkRows(t *testing.T) {
	columns := []*Ydb.Column{
		{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)},
		{Name: "score", Type: wireOptional(Ydb.Type_INT32)},
		{Name: "active", Type: wireOptional(Ydb.Type_BOOL)},
		{Name: "amount", Type: wireOptional(Ydb.Type_DOUBLE)},
		{Name: "name", Type: wireOptional(Ydb.Type_UTF8)},
		{Name: "payload", Type: wireOptional(Ydb.Type_STRING)},
	}
	original := &Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		ResultSet: &Ydb.ResultSet{Columns: columns, Rows: []*Ydb.Value{
			{Items: []*Ydb.Value{
				{Value: &Ydb.Value_Uint64Value{Uint64Value: 7}},
				{Value: &Ydb.Value_Int32Value{Int32Value: -12}},
				{Value: &Ydb.Value_BoolValue{BoolValue: true}},
				{Value: &Ydb.Value_DoubleValue{DoubleValue: 2.5}},
				{Value: &Ydb.Value_TextValue{TextValue: "alice"}},
				{Value: &Ydb.Value_BytesValue{BytesValue: []byte("binary")}},
			}},
			{Items: []*Ydb.Value{
				{Value: &Ydb.Value_Uint64Value{Uint64Value: 8}},
				{Value: &Ydb.Value_NullFlagValue{}},
				{Value: &Ydb.Value_NullFlagValue{}},
				{Value: &Ydb.Value_NullFlagValue{}},
				{Value: &Ydb.Value_NullFlagValue{}},
				{Value: &Ydb.Value_NullFlagValue{}},
			}},
		}},
	}
	frame, err := proto.Marshal(original)
	require.NoError(t, err)

	part, err := decodeWirePart(frame)
	require.NoError(t, err)
	require.Equal(t, original.GetStatus(), part.Meta().GetStatus())
	for i, column := range columns {
		require.True(t, proto.Equal(column, part.Meta().GetResultSet().GetColumns()[i]))
	}
	require.Len(t, part.rows, 2)

	var id uint64
	var score *int32
	var active *bool
	var amount *float64
	var name *string
	var payload *[]byte
	dst := []any{&id, &score, &active, &amount, &name, &payload}
	require.NoError(t, part.row(0, columns).Scan(dst...))
	require.Equal(t, uint64(7), id)
	require.Equal(t, int32(-12), *score)
	require.True(t, *active)
	require.Equal(t, 2.5, *amount)
	require.Equal(t, "alice", *name)
	require.Equal(t, []byte("binary"), *payload)

	require.NoError(t, part.row(1, columns).Scan(dst...))
	require.Equal(t, uint64(8), id)
	require.Nil(t, score)
	require.Nil(t, active)
	require.Nil(t, amount)
	require.Nil(t, name)
	require.Nil(t, payload)
}

func TestDecodePartPreservesMetadataAndUnknownFields(t *testing.T) {
	original := &Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 2,
		ResultSet: &Ydb.ResultSet{
			Format:    Ydb.ResultSet_FORMAT_VALUE,
			Truncated: true,
			Columns:   []*Ydb.Column{{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)}},
			Rows:      []*Ydb.Value{{Items: []*Ydb.Value{{Value: &Ydb.Value_Uint64Value{Uint64Value: 13}}}}},
		},
	}
	frame, err := proto.Marshal(original)
	require.NoError(t, err)
	unknown := protowire.AppendTag(nil, 200, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 9)
	frame = append(frame, unknown...)
	part, err := decodeWirePart(frame)
	require.NoError(t, err)
	require.Equal(t, int64(2), part.Meta().GetResultSetIndex())
	require.True(t, part.Meta().GetResultSet().GetTruncated())
	require.Equal(t, unknown, []byte(part.Meta().ProtoReflect().GetUnknown()))
	require.Empty(t, part.Meta().GetResultSet().GetRows())
	require.Equal(t, 1, part.RowCount())

	for i := range frame {
		frame[i] = 0
	}
	var id uint64
	require.NoError(t, part.row(0, part.Meta().GetResultSet().GetColumns()).Scan(&id))
	require.Equal(t, uint64(13), id)
}

func TestDecodePartRejectsTruncatedResponse(t *testing.T) {
	frame := protowire.AppendTag(nil, 4, protowire.BytesType)
	frame = protowire.AppendVarint(frame, 10)
	frame = append(frame, 1)
	_, err := decodeWirePart(frame)
	require.Error(t, err)
}

func TestWireRowHandlesUnknownAndMalformedFields(t *testing.T) {
	columns := []*Ydb.Column{{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)}}
	row, err := proto.Marshal(&Ydb.Value{Items: []*Ydb.Value{
		{Value: &Ydb.Value_Uint64Value{Uint64Value: 42}},
	}})
	require.NoError(t, err)
	row = protowire.AppendTag(row, 200, protowire.VarintType)
	row = protowire.AppendVarint(row, 9)

	t.Run("unknown field", func(t *testing.T) {
		part := &wirePart{frame: row, rows: []rowSpan{{end: uint32(len(row))}}}
		var got uint64
		require.NoError(t, part.row(0, columns).Scan(&got))
		require.Equal(t, uint64(42), got)
	})
	for name, frame := range map[string][]byte{
		"invalid tag":     {0xff},
		"truncated value": {0x62, 0x7f},
	} {
		t.Run(name, func(t *testing.T) {
			part := &wirePart{frame: frame, rows: []rowSpan{{end: uint32(len(frame))}}}
			var got uint64
			wireRow := part.row(0, columns)
			require.Error(t, wireRow.Scan(&got))
			require.Error(t, wireRow.ScanNamed(scanner.NamedRef("id", &got)))
		})
	}
}

func TestWireValueRowOtherScannersAndFallback(t *testing.T) {
	original := &Ydb_Query.ExecuteQueryResponsePart{ResultSet: &Ydb.ResultSet{
		Columns: []*Ydb.Column{
			{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)},
			{Name: "count", Type: wirePrimitive(Ydb.Type_UINT32)},
		},
		Rows: []*Ydb.Value{{Items: []*Ydb.Value{
			{Value: &Ydb.Value_Uint64Value{Uint64Value: 5}},
			{Value: &Ydb.Value_Uint32Value{Uint32Value: 9}},
		}}},
	}}
	frame, err := proto.Marshal(original)
	require.NoError(t, err)
	part, err := decodeWirePart(frame)
	require.NoError(t, err)
	row := part.row(0, part.Meta().GetResultSet().GetColumns())

	var id uint64
	var count uint32
	require.NoError(t, row.ScanNamed(scanner.NamedRef("count", &count), scanner.NamedRef("id", &id)))
	require.Equal(t, uint64(5), id)
	require.Equal(t, uint32(9), count)

	var dst struct {
		ID    uint64 `sql:"id"`
		Count uint32 `sql:"count"`
	}
	require.NoError(t, row.ScanStruct(&dst))
	require.Equal(t, uint64(5), dst.ID)
	require.Equal(t, uint32(9), dst.Count)
	values := row.Values()
	require.Len(t, values, 2)
	var fromValues uint64
	require.NoError(t, value.CastTo(values[0], &fromValues))
	require.Equal(t, uint64(5), fromValues)
}

func TestWireValueRowScansComplexTypesWithoutProtobufValues(t *testing.T) {
	values := []value.Value{
		value.BoolValue(true),
		value.Uint32Value(12),
		value.DecimalValue(value.BigEndianUint128(1, 2), 22, 9),
		value.OptionalValue(value.NullValue(types.Int32)),
		value.ListValue(value.Int32Value(1), value.Int32Value(2)),
		value.DictValue(value.DictValueField{K: value.TextValue("key"), V: value.Int64Value(9)}),
		value.VariantValueTuple(value.TextValue("selected"), 1, types.NewTuple(types.Int32, types.Text)),
		value.PgValue(25, "text"),
	}
	columns := make([]*Ydb.Column, len(values))
	items := make([]*Ydb.Value, len(values))
	for i, v := range values {
		pb := value.ToYDB(v)
		columns[i] = &Ydb.Column{Name: fmt.Sprintf("column_%d", i), Type: pb.GetType()}
		items[i] = pb.GetValue()
	}
	original := &Ydb_Query.ExecuteQueryResponsePart{ResultSet: &Ydb.ResultSet{
		Columns: columns, Rows: []*Ydb.Value{{Items: items}},
	}}
	frame, err := proto.Marshal(original)
	require.NoError(t, err)
	part, err := decodeWirePart(frame)
	require.NoError(t, err)
	row := part.row(0, columns)
	require.Empty(t, part.Meta().GetResultSet().GetRows())

	scanned := make([]value.Value, len(values))
	dst := make([]any, len(values))
	for i := range scanned {
		dst[i] = &scanned[i]
	}
	require.NoError(t, row.Scan(dst...))
	returned := row.Values()
	for i, want := range values {
		require.Equal(t, want.Type().Yql(), scanned[i].Type().Yql(), "scan column %d", i)
		require.Equal(t, want.Yql(), scanned[i].Yql(), "scan column %d", i)
		require.Equal(t, want.Yql(), returned[i].Yql(), "values column %d", i)
	}
}

func wirePrimitive(id Ydb.Type_PrimitiveTypeId) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: id}}
}

func wireOptional(id Ydb.Type_PrimitiveTypeId) *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: wirePrimitive(id)}}}
}
