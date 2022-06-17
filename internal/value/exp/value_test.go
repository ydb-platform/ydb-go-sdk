package value

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
	"testing"
)

func BenchmarkMemory(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		func() {
			a := allocator.New()
			defer a.Free()
			_ = typedValue(TupleValue(
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
			), a)
		}()
	}
}
