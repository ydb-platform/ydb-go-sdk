package value

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestFromWireScalarsMatchProtobufFields(t *testing.T) {
	for name, original := range wireTestValues() {
		typeOf := original.Type()
		switch typeOf.(type) {
		case types.Primitive, *types.Decimal, *types.PgType:
		default:
			continue
		}
		for _, tt := range []struct {
			name      string
			fragments []*Ydb.Value
		}{
			{name: "empty", fragments: []*Ydb.Value{{}}},
			{name: "bool", fragments: []*Ydb.Value{ToYDB(BoolValue(true)).GetValue()}},
			{name: "int32", fragments: []*Ydb.Value{ToYDB(Int32Value(-32)).GetValue()}},
			{name: "uint32", fragments: []*Ydb.Value{ToYDB(Uint32Value(32)).GetValue()}},
			{name: "int64", fragments: []*Ydb.Value{ToYDB(Int64Value(-64)).GetValue()}},
			{name: "uint64", fragments: []*Ydb.Value{ToYDB(Uint64Value(64)).GetValue()}},
			{name: "float", fragments: []*Ydb.Value{ToYDB(FloatValue(1.25)).GetValue()}},
			{name: "double", fragments: []*Ydb.Value{ToYDB(DoubleValue(2.5)).GetValue()}},
			{name: "text", fragments: []*Ydb.Value{ToYDB(TextValue("[]")).GetValue()}},
			{name: "bytes", fragments: []*Ydb.Value{ToYDB(BytesValue([]byte("[]"))).GetValue()}},
			{name: "low128", fragments: []*Ydb.Value{ToYDB(UUIDFromYDBPair(1, 2)).GetValue()}},
			{name: "null", fragments: []*Ydb.Value{{Value: &Ydb.Value_NullFlagValue{NullFlagValue: 7}}}},
			{name: "nested", fragments: []*Ydb.Value{nestedWireList(1)}},
			{
				name:      "text overwritten by number",
				fragments: []*Ydb.Value{ToYDB(TextValue("[]")).GetValue(), ToYDB(Uint64Value(1)).GetValue()},
			},
			{
				name:      "number overwritten by text",
				fragments: []*Ydb.Value{ToYDB(BoolValue(true)).GetValue(), ToYDB(TextValue("[]")).GetValue()},
			},
		} {
			t.Run(name+"/"+tt.name, func(t *testing.T) {
				fragments := append([]*Ydb.Value{{High_128: 17, VariantIndex: 3}}, tt.fragments...)
				fragments = append(fragments, &Ydb.Value{High_128: 42, VariantIndex: 4})
				data := marshalWireFragments(t, fragments...)
				var pb Ydb.Value
				if err := proto.Unmarshal(data, &pb); err != nil {
					t.Fatal(err)
				}
				want, wantErr := fromYDB(typeOf.ToYDB(), &pb)
				got, err := FromWire(typeOf, data)
				if (err == nil) != (wantErr == nil) {
					t.Fatalf("FromWire() error = %v, protobuf error = %v", err, wantErr)
				}
				if err == nil && !reflect.DeepEqual(got, want) {
					t.Fatalf("FromWire() = %s, want %s", got.Yql(), want.Yql())
				}
			})
		}
	}
}

func TestFromWireMergesRepeatedNestedMessages(t *testing.T) {
	for _, tt := range []struct {
		name      string
		fragments []*Ydb.Value
		want      string
	}{
		{name: "merge", fragments: []*Ydb.Value{nestedWireList(1), nestedWireList(2)}, want: "Just([1,2])"},
		{
			name: "another oneof arm resets the message",
			fragments: []*Ydb.Value{
				nestedWireList(1), ToYDB(Int32Value(0)).GetValue(), nestedWireList(2),
			},
			want: "Just([2])",
		},
		{
			name:      "independent field preserves the message",
			fragments: []*Ydb.Value{nestedWireList(1), {High_128: 1}, nestedWireList(2)},
			want:      "Just([1,2])",
		},
		{name: "empty first message", fragments: []*Ydb.Value{nestedWireList(), nestedWireList(2)}, want: "Just([2])"},
		{name: "empty last message", fragments: []*Ydb.Value{nestedWireList(1), nestedWireList()}, want: "Just([1])"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := marshalWireFragments(t, tt.fragments...)
			original := bytes.Clone(data)
			var pb Ydb.Value
			if err := proto.Unmarshal(data, &pb); err != nil {
				t.Fatal(err)
			}
			typeOf := types.NewOptional(types.NewList(types.Int32))
			want := FromYDB(typeOf.ToYDB(), &pb)
			got, err := FromWire(typeOf, data)
			if err != nil {
				t.Fatal(err)
			}
			if got.Yql() != tt.want || !reflect.DeepEqual(got, want) {
				t.Fatalf("FromWire() = %s, want %s (protobuf: %s)", got.Yql(), tt.want, want.Yql())
			}
			if !bytes.Equal(data, original) {
				t.Fatal("FromWire modified the response bytes")
			}
		})
	}
}

func TestFromWireMergesRepeatedPairMessages(t *testing.T) {
	key := ToYDB(TextValue("key")).GetValue()
	payload := ToYDB(TextValue("value")).GetValue()
	for _, tt := range []struct {
		name      string
		fragments []*Ydb.ValuePair
		want      Value
	}{
		{
			name: "payload",
			fragments: []*Ydb.ValuePair{
				{Key: key, Payload: nestedWireList(1).GetNestedValue()},
				{Payload: nestedWireList(2).GetNestedValue()},
			},
			want: DictValue(DictValueField{K: TextValue("key"), V: ListValue(Int32Value(1), Int32Value(2))}),
		},
		{
			name: "key interleaved with payload",
			fragments: []*Ydb.ValuePair{
				{Key: nestedWireList(1).GetNestedValue()},
				{Payload: payload},
				{Key: nestedWireList(2).GetNestedValue()},
			},
			want: DictValue(DictValueField{K: TupleValue(Int32Value(1), Int32Value(2)), V: TextValue("value")}),
		},
		{
			name: "empty last payload",
			fragments: []*Ydb.ValuePair{
				{Key: key, Payload: nestedWireList(1).GetNestedValue()},
				{Payload: new(Ydb.Value)},
			},
			want: DictValue(DictValueField{K: TextValue("key"), V: ListValue(Int32Value(1))}),
		},
		{
			name: "empty last key",
			fragments: []*Ydb.ValuePair{
				{Key: nestedWireList(1, 2).GetNestedValue(), Payload: payload},
				{Key: new(Ydb.Value)},
			},
			want: DictValue(DictValueField{K: TupleValue(Int32Value(1), Int32Value(2)), V: TextValue("value")}),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pair := new(Ydb.ValuePair)
			pair.ProtoReflect().SetUnknown(marshalWireFragments(t, tt.fragments...))
			data := marshalWireFragments(t, &Ydb.Value{Pairs: []*Ydb.ValuePair{pair}})
			original := bytes.Clone(data)
			var pb Ydb.Value
			if err := proto.Unmarshal(data, &pb); err != nil {
				t.Fatal(err)
			}
			typeOf := tt.want.Type()
			want := FromYDB(typeOf.ToYDB(), &pb)
			got, err := FromWire(typeOf, data)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) || !reflect.DeepEqual(got, want) {
				t.Fatalf("FromWire() = %s, want %s (protobuf: %s)", got.Yql(), tt.want.Yql(), want.Yql())
			}
			if !bytes.Equal(data, original) {
				t.Fatal("FromWire modified the response bytes")
			}
		})
	}
}

func TestFromWireValidatesEveryKnownMessage(t *testing.T) {
	malformed := new(Ydb.Value)
	malformed.ProtoReflect().SetUnknown([]byte{0xff})
	for name, original := range wireTestValues() {
		for field, extra := range map[string]*Ydb.Value{
			"nested": {Value: &Ydb.Value_NestedValue{NestedValue: malformed}},
			"items":  {Items: []*Ydb.Value{malformed}},
			"pairs":  {Pairs: []*Ydb.ValuePair{{Key: malformed}}},
		} {
			t.Run(name+"/"+field, func(t *testing.T) {
				data := marshalWireFragments(t, ToYDB(original).GetValue(), extra)
				if err := proto.Unmarshal(data, new(Ydb.Value)); err == nil {
					t.Fatal("protobuf accepted the malformed fixture")
				}
				got, err := FromWire(original.Type(), data)
				if err == nil {
					t.Fatalf("FromWire accepted a malformed %s message as %v", field, got)
				}
			})
		}
	}
}

func TestFromWireValidatesUnusedSetPayload(t *testing.T) {
	malformed := new(Ydb.Value)
	malformed.ProtoReflect().SetUnknown([]byte{0xff})
	data := marshalWireFragments(t, &Ydb.Value{Pairs: []*Ydb.ValuePair{{
		Key:     ToYDB(TextValue("key")).GetValue(),
		Payload: &Ydb.Value{Value: &Ydb.Value_NestedValue{NestedValue: malformed}},
	}}})
	if err := proto.Unmarshal(data, new(Ydb.Value)); err == nil {
		t.Fatal("protobuf accepted the malformed fixture")
	}
	got, err := FromWire(types.NewSet(types.Text), data)
	if err == nil {
		t.Fatalf("FromWire accepted a set with a malformed payload as %v", got)
	}
}

func TestFromWireAllowsValidUnusedMessages(t *testing.T) {
	opaque := new(Ydb.Value)
	opaque.ProtoReflect().SetUnknown(protowire.AppendBytes(
		protowire.AppendTag(nil, 200, protowire.BytesType), []byte{0xff},
	))
	for name, extra := range map[string]*Ydb.Value{
		"nested": {Value: &Ydb.Value_NestedValue{NestedValue: ToYDB(TextValue("ignored")).GetValue()}},
		"items":  {Items: []*Ydb.Value{ToYDB(TextValue("ignored")).GetValue()}},
		"pairs": {Pairs: []*Ydb.ValuePair{{
			Key: ToYDB(TextValue("key")).GetValue(), Payload: ToYDB(ListValue(Int32Value(1))).GetValue(),
		}}},
		"unknown bytes":             opaque,
		"nested with unknown bytes": {Value: &Ydb.Value_NestedValue{NestedValue: opaque}},
	} {
		t.Run(name, func(t *testing.T) {
			data := marshalWireFragments(t, ToYDB(Int32Value(42)).GetValue(), extra)
			var normal Ydb.Value
			if err := proto.Unmarshal(data, &normal); err != nil {
				t.Fatal(err)
			}
			got, err := FromWire(types.Int32, data)
			if err != nil {
				t.Fatal(err)
			}
			want := FromYDB(types.Int32.ToYDB(), &normal)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("FromWire() = %v, want %v", got, want)
			}
		})
	}
}

func TestFromWireRejectsMalformedRepeatedMessages(t *testing.T) {
	malformed := new(Ydb.Value)
	malformed.ProtoReflect().SetUnknown([]byte{0xff})
	nested := &Ydb.Value{Value: &Ydb.Value_NestedValue{NestedValue: malformed}}
	pair := new(Ydb.ValuePair)
	pair.ProtoReflect().SetUnknown(marshalWireFragments(t,
		&Ydb.ValuePair{Key: ToYDB(TextValue("key")).GetValue(), Payload: new(Ydb.Value)},
		&Ydb.ValuePair{Payload: malformed},
	))
	for _, tt := range []struct {
		name   string
		typeOf types.Type
		data   []byte
	}{
		{
			name:   "first nested body",
			typeOf: types.NewOptional(types.NewList(types.Int32)),
			data:   marshalWireFragments(t, nested, nestedWireList(1)),
		},
		{
			name:   "last nested body",
			typeOf: types.NewOptional(types.NewList(types.Int32)),
			data:   marshalWireFragments(t, nestedWireList(1), nested),
		},
		{
			name:   "pair payload body",
			typeOf: types.NewDict(types.Text, types.NewList(types.Int32)),
			data:   marshalWireFragments(t, &Ydb.Value{Pairs: []*Ydb.ValuePair{pair}}),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := proto.Unmarshal(tt.data, new(Ydb.Value)); err == nil {
				t.Fatal("protobuf accepted the malformed fixture")
			}
			got, err := FromWire(tt.typeOf, tt.data)
			if err == nil {
				t.Fatalf("FromWire() = %v, nil; want a wire-format error", got)
			}
		})
	}
}

func TestFromWirePreservesNamedVariantOrderInList(t *testing.T) {
	variant := types.NewVariantStruct(
		types.StructField{Name: "quantity", T: types.Int32},
		types.StructField{Name: "description", T: types.Text},
	)
	listType := types.NewList(variant)
	list := &Ydb.Value{Items: []*Ydb.Value{
		{Value: &Ydb.Value_NestedValue{NestedValue: ToYDB(Int32Value(42)).GetValue()}},
		{VariantIndex: 1, Value: &Ydb.Value_NestedValue{NestedValue: ToYDB(TextValue("apples")).GetValue()}},
	}}
	want := FromYDB(listType.ToYDB(), list)
	got, err := FromWire(listType, marshalWireFragments(t, list))
	if err != nil {
		t.Fatal(err)
	}
	if got.Yql() != want.Yql() {
		t.Fatalf("FromWire() = %s, want %s", got.Yql(), want.Yql())
	}
}

func TestFromWireVariantRequiresNestedValue(t *testing.T) {
	empty := &Ydb.Value{Value: &Ydb.Value_NestedValue{NestedValue: new(Ydb.Value)}}
	scalar := ToYDB(Int32Value(42)).GetValue()
	for name, schema := range map[string]types.Type{
		"tuple":  types.NewVariantTuple(types.Int32),
		"struct": types.NewVariantStruct(types.StructField{Name: "value", T: types.Int32}),
	} {
		for _, tt := range []struct {
			name string
			data []byte
		}{
			{name: "missing"},
			{name: "scalar", data: marshalWireFragments(t, scalar)},
			{name: "overwritten", data: marshalWireFragments(t, empty, scalar)},
		} {
			t.Run(name+"/"+tt.name, func(t *testing.T) {
				got, err := FromWire(schema, tt.data)
				if err == nil || got != nil {
					t.Fatalf("FromWire() = %v, %v; want nil and an error for missing nested_value", got, err)
				}
			})
		}
		t.Run(name+"/empty nested value", func(t *testing.T) {
			got, err := FromWire(schema, marshalWireFragments(t, empty))
			want := FromYDB(schema.ToYDB(), empty)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("FromWire() = %v, %v; want %v", got, err, want)
			}
		})
	}
}

func TestFromWireRejectsOutOfRangeVariantIndex(t *testing.T) {
	for _, tt := range []struct {
		name   string
		typeOf types.Type
	}{
		{name: "tuple", typeOf: types.NewVariantTuple(types.Int32)},
		{name: "struct", typeOf: types.NewVariantStruct(types.StructField{Name: "value", T: types.Int32})},
	} {
		for _, index := range []uint32{1, 1 << 31, math.MaxUint32} {
			t.Run(fmt.Sprintf("%s/index=%d", tt.name, index), func(t *testing.T) {
				data, err := proto.Marshal(&Ydb.Value{
					VariantIndex: index,
					Value: &Ydb.Value_NestedValue{NestedValue: &Ydb.Value{
						Value: &Ydb.Value_Int32Value{Int32Value: 42},
					}},
				})
				if err != nil {
					t.Fatal(err)
				}
				got, err := FromWire(tt.typeOf, data)
				if err == nil || !strings.Contains(err.Error(), "variant "+tt.name+" index") ||
					!strings.Contains(err.Error(), "out of range") {
					t.Fatalf("FromWire() = %v, %v; want a variant index out-of-range error", got, err)
				}
				if got != nil {
					t.Fatalf("FromWire() value = %v, want nil", got)
				}
			})
		}
	}
}

func TestFromWireMatchesProtobufForValueTypes(t *testing.T) {
	for name, original := range wireTestValues() {
		t.Run(name, func(t *testing.T) {
			pb := ToYDB(original)
			data, err := proto.Marshal(pb.GetValue())
			if err != nil {
				t.Fatal(err)
			}
			got, err := FromWire(types.TypeFromYDB(pb.GetType()), data)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("FromWire returned no value")
			}
			want := FromYDB(pb.GetType(), pb.GetValue())
			if got.Type().Yql() != want.Type().Yql() || got.Yql() != want.Yql() {
				t.Fatalf("got %s %s, want %s %s", got.Type().Yql(), got.Yql(), want.Type().Yql(), want.Yql())
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("value shape differs: got %#v, want %#v", got, want)
			}
		})
	}
}

func marshalWireFragments[M proto.Message](t *testing.T, fragments ...M) []byte {
	t.Helper()
	var data []byte
	for _, fragment := range fragments {
		encoded, err := proto.Marshal(fragment)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, encoded...)
	}

	return data
}

func nestedWireList(items ...int32) *Ydb.Value {
	values := make([]Value, len(items))
	for i, item := range items {
		values[i] = Int32Value(item)
	}

	return &Ydb.Value{Value: &Ydb.Value_NestedValue{NestedValue: ToYDB(ListValue(values...)).GetValue()}}
}

func wireTestValues() map[string]Value {
	return map[string]Value{
		"bool":            BoolValue(true),
		"int8":            Int8Value(-8),
		"int16":           Int16Value(-16),
		"int32":           Int32Value(-32),
		"int64":           Int64Value(-64),
		"uint8":           Uint8Value(8),
		"uint16":          Uint16Value(16),
		"uint32":          Uint32Value(32),
		"uint64":          Uint64Value(64),
		"float":           FloatValue(1.25),
		"double":          DoubleValue(2.5),
		"date":            DateValue(20000),
		"date32":          Date32Value(-10),
		"datetime":        DatetimeValue(100),
		"datetime64":      Datetime64Value(-100),
		"timestamp":       TimestampValue(1000),
		"timestamp64":     Timestamp64Value(-1000),
		"interval":        IntervalValue(-120),
		"interval64":      Interval64Value(-120),
		"text":            TextValue("привет"),
		"bytes":           BytesValue([]byte{0, 1, 2}),
		"json":            JSONValue(`{"a":1}`),
		"json_document":   JSONDocumentValue(`{"a":1}`),
		"yson":            YSONValue([]byte("[]")),
		"dynumber":        DyNumberValue("1.5"),
		"tzdate":          TzDateValue("2024-01-01,Europe/Moscow"),
		"tzdatetime":      TzDatetimeValue("2024-01-01T00:00:00,Europe/Moscow"),
		"tztimestamp":     TzTimestampValue("2024-01-01T00:00:00.000000,Europe/Moscow"),
		"tzdate32":        TzDate32Value("2024-01-01,Europe/Moscow"),
		"tzdatetime64":    TzDatetime64Value("2024-01-01T00:00:00,Europe/Moscow"),
		"tztimestamp64":   TzTimestamp64Value("2024-01-01T00:00:00.000000,Europe/Moscow"),
		"decimal":         DecimalValue(BigEndianUint128(1, 2), 22, 9),
		"uuid":            UUIDFromYDBPair(1, 2),
		"optional":        OptionalValue(Int32Value(42)),
		"null_optional":   NullValue(types.Int32),
		"nested_optional": OptionalValue(NullValue(types.Int32)),
		"optional_variant": OptionalValue(VariantValueTuple(Int32Value(7), 0,
			types.NewTuple(types.Int32, types.Text))),
		"list":             ListValue(Int32Value(1), Int32Value(2)),
		"empty_typed_list": ListValueWithType(types.NewList(types.Int32), []Value{}),
		"nested_list": ListValue(
			ListValueWithType(types.NewList(types.Int32), []Value{}),
			ListValue(Int32Value(1))),
		"tuple":            TupleValue(Int32Value(1), TextValue("two")),
		"struct":           StructValue(StructValueField{Name: "n", V: Int32Value(1)}),
		"dict":             DictValue(DictValueField{K: TextValue("one"), V: Int32Value(1)}),
		"empty_typed_dict": DictValueWithType(types.NewDict(types.Text, types.Int32), []DictValueField{}),
		"set":              SetValue(TextValue("one")),
		"empty_typed_set":  SetValueWithType(types.NewSet(types.Text), []Value{}),
		"tagged":           TaggedValue(types.NewTagged(types.Int32, "tag"), Int32Value(1)),
		"variant_tuple": VariantValueTuple(Int32Value(7), 0,
			types.NewTuple(types.Int32, types.Text)),
		"variant_struct": VariantValueStruct(TextValue("x"), "name",
			types.NewStruct(types.StructField{Name: "name", T: types.Text})),
		"empty_list": ListValueWithType(types.NewEmptyList(), nil),
		"empty_dict": DictValueWithType(types.NewEmptyDict(), nil),
		"void":       VoidValue(),
		"null":       LiteralNullValue(),
		"pg":         PgValue(25, "text"),
		"pg_null":    PgNullValue(25),
	}
}
