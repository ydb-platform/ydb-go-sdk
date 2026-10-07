package wirevalue

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
)

func BenchmarkWireValueDecodeScan(b *testing.B) {
	frame := wireValueBenchmarkFrame(b, 10000)
	b.Run("protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var part Ydb_Query.ExecuteQueryResponsePart
			if err := proto.Unmarshal(frame, &part); err != nil {
				b.Fatal(err)
			}
			var hash uint64
			var id uint64
			var score *int32
			var active *bool
			var amount *float64
			var name *string
			var payload *[]byte
			dst := []any{&id, &score, &active, &amount, &name, &payload}
			for _, row := range part.GetResultSet().GetRows() {
				data := scanner.NewData(part.GetResultSet().GetColumns(), row.GetItems())
				if err := scanner.Indexed(data).Scan(dst...); err != nil {
					b.Fatal(err)
				}
				hash = wireBenchChecksum(hash, id, score, active, amount, name, payload)
			}
			benchmarkWireValueHash = hash
		}
	})
	b.Run("wire", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			part, err := DecodePart(frame)
			if err != nil {
				b.Fatal(err)
			}
			var hash uint64
			var id uint64
			var score *int32
			var active *bool
			var amount *float64
			var name *string
			var payload *[]byte
			dst := []any{&id, &score, &active, &amount, &name, &payload}
			for i := range part.rows {
				if err := part.Row(i).Scan(dst...); err != nil {
					b.Fatal(err)
				}
				hash = wireBenchChecksum(hash, id, score, active, amount, name, payload)
			}
			benchmarkWireValueHash = hash
		}
	})
}

var benchmarkWireValueHash uint64

func wireBenchChecksum(h, id uint64, score *int32, active *bool, amount *float64,
	name *string, payload *[]byte,
) uint64 {
	h += id
	if score != nil {
		h += uint64(*score)
	}
	if active != nil && *active {
		h++
	}
	if amount != nil {
		h += uint64(*amount)
	}
	if name != nil {
		h += uint64(len(*name))
	}
	if payload != nil {
		h += uint64(len(*payload))
	}

	return h
}

func wireValueBenchmarkFrame(b *testing.B, count int) []byte {
	b.Helper()
	columns := []*Ydb.Column{
		{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)},
		{Name: "score", Type: wireOptional(Ydb.Type_INT32)},
		{Name: "active", Type: wireOptional(Ydb.Type_BOOL)},
		{Name: "amount", Type: wireOptional(Ydb.Type_DOUBLE)},
		{Name: "name", Type: wireOptional(Ydb.Type_UTF8)},
		{Name: "payload", Type: wireOptional(Ydb.Type_STRING)},
	}
	rows := make([]*Ydb.Value, count)
	for i := range rows {
		rows[i] = &Ydb.Value{Items: []*Ydb.Value{
			{Value: &Ydb.Value_Uint64Value{Uint64Value: uint64(i)}},
			{Value: &Ydb.Value_Int32Value{Int32Value: int32(i % 1000)}},
			{Value: &Ydb.Value_BoolValue{BoolValue: i%2 == 0}},
			{Value: &Ydb.Value_DoubleValue{DoubleValue: float64(i) / 4}},
			{Value: &Ydb.Value_TextValue{TextValue: fmt.Sprintf("customer-%05d", i)}},
			{Value: &Ydb.Value_BytesValue{BytesValue: bytes.Repeat([]byte("x"), 64)}},
		}}
	}
	frame, err := proto.Marshal(&Ydb_Query.ExecuteQueryResponsePart{
		Status:    Ydb.StatusIds_SUCCESS,
		ResultSet: &Ydb.ResultSet{Format: Ydb.ResultSet_FORMAT_VALUE, Columns: columns, Rows: rows},
	})
	if err != nil {
		b.Fatal(err)
	}

	return frame
}
