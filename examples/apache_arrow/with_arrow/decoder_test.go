package witharrow

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
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
	alloc.AssertSize(t, 0)
	columns := []query.ArrowColumn{{Name: "id", Type: types.TypeInt32}, {Name: "name", Type: types.Optional(types.TypeText)}, {Name: "payload", Type: types.TypeBytes}}
	decode := query.NewArrowDecoder(ipc.NewReader, ipc.WithAllocator(alloc))
	batches, err := decode(context.Background(), columns, bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Decode(cancelled, columns, bytes.NewReader(wire.Bytes())); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled decode: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("batches=%d", len(batches))
	}
	var rows [][]types.Value
	var scannedNames []*string
	var scannedPayloads [][]byte
	for _, b := range batches {
		if b.NumCols() != 3 {
			t.Fatal("reader released batch before result Close")
		}
		var name *string
		var payload []byte
		if err := b.Scan(0, 1, &name); err != nil {
			t.Fatal(err)
		}
		if err := b.Scan(0, 2, &payload); err != nil {
			t.Fatal(err)
		}
		scannedNames = append(scannedNames, name)
		scannedPayloads = append(scannedPayloads, payload)
		rows = append(rows, []types.Value{b.Value(0, 0), b.Value(0, 1), b.Value(0, 2)})
		b.Release()
	}
	alloc.AssertSize(t, 0)
	clear(wire.Bytes())
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
		if !reflect.DeepEqual(scannedNames[i], name) || !bytes.Equal(scannedPayloads[i], payload) {
			t.Fatal("Scan output lost owned data")
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
		batch, err := decodeColumn(t, nonNull, types.TypeBool)
		if err != nil {
			t.Fatal(err)
		}
		var got bool
		if err := types.CastTo(batch.Value(i, 0), &got); err != nil || got != want {
			t.Fatalf("bool=%v want=%v err=%v", got, want, err)
		}
		batch.Release()
	}
	for _, typ := range []types.Type{
		types.TypeBool,
		types.TypeText,
		types.TypeDate,
		types.Optional(types.Optional(types.TypeBool)),
	} {
		if _, err := decodeColumn(t, data, typ); err == nil {
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
			batch, err := decodeColumn(t, nonNull, test.want.Type())
			if err != nil {
				t.Fatal(err)
			}
			if got := batch.Value(0, 0); !types.Equal(got.Type(), test.want.Type()) || got.Yql() != test.want.Yql() {
				t.Fatalf("scalar=%s, want %s", got, test.want)
			}
			batch.Release()
			for _, nullable := range []arrow.Array{nonNull, data} {
				batch, err = decodeColumn(t, nullable, types.Optional(test.want.Type()))
				if err != nil {
					t.Fatal(err)
				}
				wantValues := []types.Value{types.OptionalValue(test.want), types.NullValue(test.want.Type())}
				for i, want := range wantValues[:nullable.Len()] {
					if got := batch.Value(i, 0); !types.Equal(got.Type(), want.Type()) || got.Yql() != want.Yql() {
						t.Fatalf("optional[%d]=%s, want %s", i, got, want)
					}
				}
				var scalar driver.Value
				if err := types.CastTo(test.want, &scalar); err != nil {
					t.Fatal(err)
				}
				dst := reflect.New(reflect.PointerTo(reflect.TypeOf(scalar)))
				native := reflect.ValueOf(nullable).MethodByName("Value").Call([]reflect.Value{reflect.ValueOf(0)})[0].Interface()
				nativeDst := reflect.New(reflect.PointerTo(reflect.TypeOf(native)))
				directDst := reflect.New(reflect.TypeOf(native))
				if err := batch.Scan(0, 0, directDst.Interface()); err != nil || !reflect.DeepEqual(directDst.Elem().Interface(), native) {
					t.Fatalf("native scalar scan: %v", err)
				}
				for i := 0; i < nullable.Len(); i++ {
					if err := batch.Scan(i, 0, dst.Interface()); err != nil {
						t.Fatal(err)
					}
					got := dst.Elem()
					if i == 1 {
						if !got.IsNil() {
							t.Fatal("direct optional scan lost null")
						}
					} else if got.IsNil() || !reflect.DeepEqual(got.Elem().Interface(), scalar) {
						t.Fatalf("direct scan=%v, want %v", got, scalar)
					}
					if err := batch.Scan(i, 0, nativeDst.Interface()); err != nil {
						t.Fatal(err)
					}
					if i == 1 {
						if !nativeDst.Elem().IsNil() {
							t.Fatal("native optional scan lost null")
						}
					} else if nativeDst.Elem().IsNil() || !reflect.DeepEqual(nativeDst.Elem().Elem().Interface(), native) {
						t.Fatal("native optional scan differs")
					}
					var expected, actual string
					if err := types.CastTo(batch.Value(i, 0), &expected); err != nil {
						t.Fatal(err)
					}
					if err := batch.Scan(i, 0, &actual); err != nil || actual != expected {
						t.Fatalf("fallback=%q, want %q, err=%v", actual, expected, err)
					}
				}

				batch.Release()
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
			batch, err := decodeColumn(t, data, types.Optional(types.TypeBool))
			if err != nil {
				data.Release()
				t.Fatal(err)
			}
			values := make([]types.Value, data.Len())
			for i := range values {
				values[i] = batch.Value(i, 0)
				var got *bool
				if err := batch.Scan(i, 0, &got); err != nil {
					t.Fatal(err)
				}
				want := i == 1 || i == 2
				if i == 3 {
					if got != nil {
						t.Fatal("Scan lost bool null")
					}
				} else if got == nil || *got != want {
					t.Fatalf("Scan bool[%d]=%v", i, got)
				}
			}
			data.Release()
			batch.Release()
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
			if _, err := decodeColumn(t, data, typ); err == nil {
				t.Errorf("expected error for %s with %s", typ, json)
			}
		}
		data.Release()
	}
}

func TestDecodeEmptyBatch(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	record := builder.NewRecordBatch()
	defer record.Release()
	var wire bytes.Buffer
	writer := ipc.NewWriter(&wire, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if wire.Len() == 0 {
		t.Fatal("empty batch must have an IPC payload")
	}
	columns := []query.ArrowColumn{{Name: "id", Type: types.TypeInt32}}
	batches, err := Decode(t.Context(), columns, bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 {
		t.Fatalf("batches=%d, want 1", len(batches))
	}
	if batches[0].NumRows() != 0 || batches[0].NumCols() != 1 {
		t.Fatal("empty batch dimensions differ")
	}
	batches[0].Release()
	columns[0].Type = types.TypeText
	batches, err = Decode(t.Context(), columns, bytes.NewReader(wire.Bytes()))
	if err == nil || !strings.Contains(err.Error(), `column "id"`) || len(batches) != 0 {
		t.Fatalf("type mismatch must fail before returning batches: %v, %v", batches, err)
	}
}

func decodeColumn(t testing.TB, data arrow.Array, typ types.Type) (query.ArrowBatch, error) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: data.DataType(), Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{data}, int64(data.Len()))
	defer record.Release()
	var wire bytes.Buffer
	writer := ipc.NewWriter(&wire, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	batches, err := Decode(context.Background(), []query.ArrowColumn{{Name: "value", Type: typ}}, bytes.NewReader(wire.Bytes()))
	if err != nil {
		return nil, err
	}
	if len(batches) != 1 {
		t.Fatalf("batches=%d, want 1", len(batches))
	}
	return batches[0], nil
}
