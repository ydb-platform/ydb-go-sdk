package value

import (
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
	"google.golang.org/protobuf/proto"
)

func BenchmarkMemory(b *testing.B) {
	b.ReportAllocs()
	v := TupleValue(
		BoolValue(true),
		Int8Value(1),
		Int16Value(1),
		Int32Value(1),
		Int64Value(1),
		Uint8Value(1),
		Uint16Value(1),
		Uint32Value(1),
		Uint64Value(1),
		DateValue(1),
		DatetimeValue(1),
		TimestampValue(1),
		IntervalValue(1),
		VoidValue(),
		// types with non-zero allocations
		FloatValue(1),
		DoubleValue(1),
		StringValue([]byte("test")),
		DecimalValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}, 22, 9),
		DyNumberValue("123"),
		JSONValue("{}"),
		JSONDocumentValue("{}"),
		TzDateValue("1"),
		TzDatetimeValue("1"),
		TzTimestampValue("1"),
		UTF8Value("1"),
		UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}),
		YSONValue("{}"),
		ListValue(
			Int64Value(1),
			Int64Value(2),
			Int64Value(3),
		),
		OptionalValue(IntervalValue(1)),
		OptionalValue(OptionalValue(IntervalValue(1))),
		StructValue(
			StructField("series_id", Uint64Value(1)),
			StructField("title", UTF8Value("test")),
			StructField("air_date", DateValue(1)),
			StructField("remove_date", OptionalValue(TzDatetimeValue("1234"))),
		),
		DictValue(
			DictField(UTF8Value("series_id"), Uint64Value(1)),
			DictField(UTF8Value("title"), Uint64Value(2)),
			DictField(UTF8Value("air_date"), Uint64Value(3)),
			DictField(UTF8Value("remove_date"), Uint64Value(4)),
		),
	)
	for i := 0; i < b.N; i++ {
		a := allocator.New()
		_ = typedValue(v, a)
		a.Free()
	}
}

func TestCompareProtos(t *testing.T) {
	a := allocator.New()
	for _, tt := range []struct {
		old *Ydb.TypedValue
		new *Ydb.TypedValue
	}{
		{
			value.ToYDB(value.BoolValue(true)),
			typedValue(BoolValue(true), a),
		},
		{
			value.ToYDB(value.DateValue(1)),
			typedValue(DateValue(1), a),
		},
		{
			value.ToYDB(value.DatetimeValue(1)),
			typedValue(DatetimeValue(1), a),
		},
		{
			value.ToYDB(
				value.DecimalValue(value.DecimalType{
					Precision: 22,
					Scale:     9,
				},
					[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6},
				),
			),
			typedValue(
				DecimalValue(
					[...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6},
					22,
					9,
				),
				a,
			),
		},
		{
			value.ToYDB(value.DoubleValue(1)),
			typedValue(DoubleValue(1), a),
		},
		{
			value.ToYDB(value.DyNumberValue("1")),
			typedValue(DyNumberValue("1"), a),
		},
		{
			value.ToYDB(value.FloatValue(1)),
			typedValue(FloatValue(1), a),
		},
		{
			value.ToYDB(value.Int8Value(1)),
			typedValue(Int8Value(1), a),
		},
		{
			value.ToYDB(value.Int16Value(1)),
			typedValue(Int16Value(1), a),
		},
		{
			value.ToYDB(value.Int32Value(1)),
			typedValue(Int32Value(1), a),
		},
		{
			value.ToYDB(value.Int64Value(1)),
			typedValue(Int64Value(1), a),
		},
		{
			value.ToYDB(value.IntervalValue(1)),
			typedValue(IntervalValue(1), a),
		},
		{
			value.ToYDB(value.JSONValue("1")),
			typedValue(JSONValue("1"), a),
		},
		{
			value.ToYDB(value.JSONDocumentValue("1")),
			typedValue(JSONDocumentValue("1"), a),
		},
		{
			value.ToYDB(value.ListValue(1, func(i int) value.V {
				return value.Int8Value(1)
			})),
			typedValue(ListValue(
				Int8Value(1),
			), a),
		},
		{
			value.ToYDB(value.StringValue([]byte("test"))),
			typedValue(StringValue([]byte("test")), a),
		},
		{
			value.ToYDB(value.TimestampValue(1)),
			typedValue(TimestampValue(1), a),
		},
		{
			value.ToYDB(value.TupleValue(2, func(i int) value.V {
				switch i {
				case 0:
					return value.Int8Value(1)
				case 1:
					return value.FloatValue(1)
				default:
					return nil
				}
			})),
			typedValue(TupleValue(
				Int8Value(1),
				FloatValue(1),
			), a),
		},
		{
			value.ToYDB(value.TzDateValue("1")),
			typedValue(TzDateValue("1"), a),
		},
		{
			value.ToYDB(value.TzDatetimeValue("1")),
			typedValue(TzDatetimeValue("1"), a),
		},
		{
			value.ToYDB(value.TzTimestampValue("1")),
			typedValue(TzTimestampValue("1"), a),
		},
		{
			value.ToYDB(value.Uint8Value(1)),
			typedValue(Uint8Value(1), a),
		},
		{
			value.ToYDB(value.Uint16Value(1)),
			typedValue(Uint16Value(1), a),
		},
		{
			value.ToYDB(value.Uint32Value(1)),
			typedValue(Uint32Value(1), a),
		},
		{
			value.ToYDB(value.Uint64Value(1)),
			typedValue(Uint64Value(1), a),
		},
		{
			value.ToYDB(value.UTF8Value("test")),
			typedValue(UTF8Value("test"), a),
		},
		{
			value.ToYDB(value.UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6})),
			typedValue(UUIDValue([...]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6}), a),
		},
		{
			value.ToYDB(value.VoidValue),
			typedValue(VoidValue(), a),
		},
		{
			value.ToYDB(value.YSONValue("1")),
			typedValue(YSONValue("1"), a),
		},
		{
			value.ToYDB(value.OptionalValue(value.IntervalValue(1))),
			typedValue(OptionalValue(IntervalValue(1)), a),
		},
		{
			value.ToYDB(value.OptionalValue(value.OptionalValue(value.IntervalValue(1)))),
			typedValue(OptionalValue(OptionalValue(IntervalValue(1))), a),
		},
		{
			value.ToYDB(value.StructValue(
				&value.StructValueProto{
					Fields: []value.StructField{
						{
							Name: "series_id",
							Type: value.TypeFromYDB(value.Uint64Value(1).ToYDB().Type),
						},
						{
							Name: "title",
							Type: value.TypeFromYDB(value.UTF8Value("test").ToYDB().Type),
						},
						{
							Name: "air_date",
							Type: value.TypeFromYDB(value.DateValue(1).ToYDB().Type),
						},
						{
							Name: "remove_date",
							Type: value.TypeFromYDB(
								value.OptionalValue(value.TzDatetimeValue("1234")).ToYDB().Type,
							),
						},
					},
					Values: []*Ydb.Value{
						value.Uint64Value(1).ToYDB().Value,
						value.UTF8Value("test").ToYDB().Value,
						value.DateValue(1).ToYDB().Value,
						value.OptionalValue(value.TzDatetimeValue("1234")).ToYDB().Value,
					},
				},
			)),
			typedValue(StructValue(
				StructField("series_id", Uint64Value(1)),
				StructField("title", UTF8Value("test")),
				StructField("air_date", DateValue(1)),
				StructField("remove_date", OptionalValue(TzDatetimeValue("1234"))),
			), a),
		},
		{
			value.ToYDB(value.DictValue(8, func(i int) value.V {
				switch i {
				// Key items.
				case 0:
					return value.UTF8Value("series_id")
				case 2:
					return value.UTF8Value("title")
				case 4:
					return value.UTF8Value("air_date")
				case 6:
					return value.UTF8Value("remove_date")
				}
				// Value items.
				switch i {
				case 1:
					return value.Uint64Value(1)
				case 3:
					return value.Uint64Value(2)
				case 5:
					return value.Uint64Value(3)
				case 7:
					return value.Uint64Value(4)
				}
				panic("whoa")
			})),
			typedValue(DictValue(
				DictField(UTF8Value("series_id"), Uint64Value(1)),
				DictField(UTF8Value("title"), Uint64Value(2)),
				DictField(UTF8Value("air_date"), Uint64Value(3)),
				DictField(UTF8Value("remove_date"), Uint64Value(4)),
			), a),
		},
	} {
		t.Run(tt.old.String(), func(t *testing.T) {
			if !proto.Equal(tt.old, tt.new) {
				t.Errorf("not equal: %v != %v", tt.old, tt.new)
			}
		})
	}
}
