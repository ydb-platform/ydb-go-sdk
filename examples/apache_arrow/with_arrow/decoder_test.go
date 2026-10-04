package witharrow

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestDecodeOwnsValues(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}, {Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}, {Name: "payload", Type: arrow.BinaryTypes.Binary}}, nil)
	builder := array.NewRecordBuilder(alloc, schema)
	var wire bytes.Buffer
	writer := ipc.NewWriter(&wire, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	for i := 0; i < 2; i++ {
		builder.Field(0).(*array.Int32Builder).Append(int32(i + 1))
		if i == 0 {
			builder.Field(1).(*array.StringBuilder).Append("owned text")
		} else {
			builder.Field(1).(*array.StringBuilder).AppendNull()
		}
		builder.Field(2).(*array.BinaryBuilder).Append([]byte("owned bytes"))
		batch := builder.NewRecordBatch()
		if err := writer.Write(batch); err != nil {
			t.Fatal(err)
		}
		batch.Release()
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	builder.Release()
	columns := []query.ArrowColumn{{Name: "id", Type: types.TypeInt32}, {Name: "name", Type: types.Optional(types.TypeText)}, {Name: "payload", Type: types.TypeBytes}}
	rows, err := Decode(context.Background(), columns, bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	alloc.AssertSize(t, 0)
	clear(wire.Bytes())
	if len(rows) != 2 {
		t.Fatalf("rows=%d", len(rows))
	}
	for i, row := range rows {
		var id int32
		var name *string
		var payload []byte
		if err := types.CastTo(row[0], &id); err != nil {
			t.Fatal(err)
		}
		if err := types.CastTo(row[1], &name); err != nil {
			t.Fatal(err)
		}
		if err := types.CastTo(row[2], &payload); err != nil {
			t.Fatal(err)
		}
		if id != int32(i+1) || string(payload) != "owned bytes" {
			t.Fatalf("lost values: %d %q", id, payload)
		}
		if i == 0 && (name == nil || *name != "owned text") {
			t.Fatalf("lost text: %v", name)
		}
		if i == 1 && name != nil {
			t.Fatalf("lost null: %v", name)
		}
	}
}

func TestDecodeInvalidIPC(t *testing.T) {
	if _, err := Decode(context.Background(), nil, bytes.NewReader([]byte("invalid"))); err == nil {
		t.Fatal("expected IPC error")
	}
}

func TestBoolAndInvalidTypes(t *testing.T) {
	builder := array.NewUint8Builder(memory.DefaultAllocator)
	builder.AppendValues([]uint8{0, 1, 0}, []bool{true, true, false})
	data := builder.NewArray()
	builder.Release()
	defer data.Release()
	for i, want := range []bool{false, true} {
		v, err := columnValue(data, i, types.TypeBool)
		if err != nil {
			t.Fatal(err)
		}
		var got bool
		if err := types.CastTo(v, &got); err != nil || got != want {
			t.Fatalf("bool=%v want=%v err=%v", got, want, err)
		}
	}
	for _, test := range []struct {
		row int
		typ types.Type
	}{
		{row: 2, typ: types.TypeBool},
		{row: 0, typ: types.TypeText},
		{row: 0, typ: types.TypeDate},
		{row: 0, typ: types.Optional(types.Optional(types.TypeBool))},
	} {
		if _, err := columnValue(data, test.row, test.typ); err == nil {
			t.Fatalf("expected error for %s row %d", test.typ, test.row)
		}
	}
}
