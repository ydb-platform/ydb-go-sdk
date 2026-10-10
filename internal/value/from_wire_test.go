package value

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

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
