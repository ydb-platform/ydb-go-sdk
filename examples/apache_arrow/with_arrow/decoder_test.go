package witharrow

import (
	"bytes"
	"context"
	"errors"
	"strings"
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
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Decode(cancelled, columns, bytes.NewReader(wire.Bytes())); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled decode: %v", err)
	}
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
	nonNull := array.NewSlice(data, 0, 2)
	defer nonNull.Release()
	for i, want := range []bool{false, true} {
		read, err := columnReader(nonNull, types.TypeBool)
		if err != nil {
			t.Fatal(err)
		}
		var got bool
		if err := types.CastTo(read(i), &got); err != nil || got != want {
			t.Fatalf("bool=%v want=%v err=%v", got, want, err)
		}
	}
	for _, typ := range []types.Type{
		types.TypeBool,
		types.TypeText,
		types.TypeDate,
		types.Optional(types.Optional(types.TypeBool)),
	} {
		if _, err := columnReader(data, typ); err == nil {
			t.Fatalf("expected error for %s", typ)
		}
	}
}

func TestColumnReaderScalarTypes(t *testing.T) {
	for _, test := range []struct {
		arrowType arrow.DataType
		json      string
		want      types.Value
	}{
		{arrow.FixedWidthTypes.Boolean, `[true, null]`, types.BoolValue(true)},
		{arrow.PrimitiveTypes.Int8, `[-7, null]`, types.Int8Value(-7)},
		{arrow.PrimitiveTypes.Int16, `[-700, null]`, types.Int16Value(-700)},
		{arrow.PrimitiveTypes.Int32, `[-70000, null]`, types.Int32Value(-70000)},
		{arrow.PrimitiveTypes.Int64, `[-70000000, null]`, types.Int64Value(-70000000)},
		{arrow.PrimitiveTypes.Uint8, `[7, null]`, types.Uint8Value(7)},
		{arrow.PrimitiveTypes.Uint16, `[700, null]`, types.Uint16Value(700)},
		{arrow.PrimitiveTypes.Uint32, `[70000, null]`, types.Uint32Value(70000)},
		{arrow.PrimitiveTypes.Uint64, `[70000000, null]`, types.Uint64Value(70000000)},
		{arrow.PrimitiveTypes.Float32, `[1.25, null]`, types.FloatValue(1.25)},
		{arrow.PrimitiveTypes.Float64, `[1.25, null]`, types.DoubleValue(1.25)},
		{arrow.BinaryTypes.String, `["owned text", null]`, types.TextValue("owned text")},
		{arrow.BinaryTypes.Binary, `["b3duZWQgYnl0ZXM=", null]`, types.BytesValue([]byte("owned bytes"))},
	} {
		t.Run(test.arrowType.String(), func(t *testing.T) {
			data, _, err := array.FromJSON(memory.DefaultAllocator, test.arrowType, strings.NewReader(test.json))
			if err != nil {
				t.Fatal(err)
			}
			defer data.Release()
			nonNull := array.NewSlice(data, 0, 1)
			defer nonNull.Release()
			read, err := columnReader(nonNull, test.want.Type())
			if err != nil {
				t.Fatal(err)
			}
			if got := read(0); !types.Equal(got.Type(), test.want.Type()) || got.Yql() != test.want.Yql() {
				t.Fatalf("scalar=%s, want %s", got, test.want)
			}
			for _, nullable := range []arrow.Array{nonNull, data} {
				read, err = columnReader(nullable, types.Optional(test.want.Type()))
				if err != nil {
					t.Fatal(err)
				}
				wantValues := []types.Value{types.OptionalValue(test.want), types.NullValue(test.want.Type())}
				for i, want := range wantValues[:nullable.Len()] {
					if got := read(i); !types.Equal(got.Type(), want.Type()) || got.Yql() != want.Yql() {
						t.Fatalf("optional[%d]=%s, want %s", i, got, want)
					}
				}
			}
		})
	}
}

func TestOptionalBoolScanDestinations(t *testing.T) {
	for _, test := range []struct {
		arrowType arrow.DataType
		json      string
	}{
		{arrow.FixedWidthTypes.Boolean, `[false, true, true, null, false]`},
		{arrow.PrimitiveTypes.Uint8, `[0, 1, 2, null, 0]`},
	} {
		t.Run(test.arrowType.String(), func(t *testing.T) {
			data, _, err := array.FromJSON(memory.DefaultAllocator, test.arrowType, strings.NewReader(test.json))
			if err != nil {
				t.Fatal(err)
			}
			read, err := columnReader(data, types.Optional(types.TypeBool))
			if err != nil {
				data.Release()
				t.Fatal(err)
			}
			values := make([]types.Value, data.Len())
			for i := range values {
				values[i] = read(i)
			}
			data.Release()
			for i, want := range []bool{false, true, true, false, false} {
				var got *bool
				if err := types.CastTo(values[i], &got); err != nil {
					t.Fatal(err)
				}
				if i == 3 {
					if got != nil {
						t.Fatalf("null=%v", got)
					}
					continue
				}
				if got == nil || *got != want {
					t.Fatalf("bool[%d]=%v want=%v", i, got, want)
				}
				*got = !want
				var again *bool
				if err := types.CastTo(values[i], &again); err != nil || again == nil || *again != want {
					t.Fatalf("modified shared value: %v err=%v", again, err)
				}
			}
		})
	}
}

func TestColumnReaderInvalidNullTypes(t *testing.T) {
	for _, json := range []string{`[null, null]`, `[]`} {
		data, _, err := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, strings.NewReader(json))
		if err != nil {
			t.Fatal(err)
		}
		for _, typ := range []types.Type{types.Optional(types.TypeText), types.Optional(types.TypeDate)} {
			if _, err := columnReader(data, typ); err == nil {
				t.Errorf("expected error for %s with %s", typ, json)
			}
		}
		data.Release()
	}
}
