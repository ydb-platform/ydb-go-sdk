package wirevalue

import (
	"bytes"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestParseValidatesTextFields(t *testing.T) {
	field := func(number protowire.Number, data []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), data)
	}
	bad := field(ValueTextField, []byte{0xff})
	good := field(ValueTextField, []byte("π"))
	for _, tt := range []struct {
		name string
		data []byte
	}{
		{name: "invalid text", data: bad},
		{name: "invalid text overwritten", data: append(bytes.Clone(bad), good...)},
		{name: "invalid nested text overwritten", data: append(field(ValueNestedField, bad), good...)},
		{name: "valid multibyte text", data: good},
		{name: "arbitrary binary data", data: field(ValueBytesField, []byte{0xff})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wantErr := proto.Unmarshal(tt.data, new(Ydb.Value))
			cell, err := Parse(tt.data)
			if (err == nil) != (wantErr == nil) {
				t.Fatalf("Parse() = %v, %v; protobuf error = %v", cell, err, wantErr)
			}
		})
	}
}

func TestCellReadsNestedValuesAndPairs(t *testing.T) {
	want := &Ydb.Value{
		Pairs: []*Ydb.ValuePair{{
			Key: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: "key"}},
			Payload: &Ydb.Value{Value: &Ydb.Value_NestedValue{NestedValue: &Ydb.Value{
				Value: &Ydb.Value_Uint64Value{Uint64Value: 42},
			}}},
		}},
		VariantIndex: 3,
	}
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if got := cell.VariantIndex(); got != 3 {
		t.Fatalf("variant index = %d, want 3", got)
	}
	pairs := cell.Pairs()
	if !pairs.Next() {
		t.Fatalf("first pair missing: %v", pairs.Err())
	}
	key, payload := pairs.Key(), pairs.Payload()
	if got := string(key.Bytes()); got != "key" {
		t.Fatalf("key = %q, want key", got)
	}
	nested, err := payload.Nested()
	if err != nil {
		t.Fatal(err)
	}
	if got := nested.Uint64(); got != 42 {
		t.Fatalf("payload = %d, want 42", got)
	}
	if pairs.Next() || pairs.Err() != nil {
		t.Fatalf("unexpected further pair or error: %v", pairs.Err())
	}
}
