package types

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestNullableValues(t *testing.T) {
	testNullableValue(t, NullableBoolValue, TypeBool, true, BoolValue)
	testNullableValue(t, NullableInt8Value, TypeInt8, int8(-8), Int8Value)
	testNullableValue(t, NullableInt16Value, TypeInt16, int16(-16), Int16Value)
	testNullableValue(t, NullableInt32Value, TypeInt32, int32(-32), Int32Value)
	testNullableValue(t, NullableInt64Value, TypeInt64, int64(-64), Int64Value)
	testNullableValue(t, NullableUint8Value, TypeUint8, uint8(8), Uint8Value)
	testNullableValue(t, NullableUint16Value, TypeUint16, uint16(16), Uint16Value)
	testNullableValue(t, NullableUint32Value, TypeUint32, uint32(32), Uint32Value)
	testNullableValue(t, NullableUint64Value, TypeUint64, uint64(64), Uint64Value)
	testNullableValue(t, NullableFloatValue, TypeFloat, float32(1.25), FloatValue)
	testNullableValue(t, NullableDoubleValue, TypeDouble, 2.5, DoubleValue)
	testNullableValue(t, NullableDateValue, TypeDate, uint32(1), DateValue)
	testNullableValue(t, NullableDate32Value, TypeDate32, int32(-1), Date32Value)
	testNullableValue(t, NullableDatetimeValue, TypeDatetime, uint32(1), DatetimeValue)
	testNullableValue(t, NullableDatetime64Value, TypeDatetime64, int64(-1), Datetime64Value)
	testNullableValue(t, NullableTimestampValue, TypeTimestamp, uint64(1), TimestampValue)
	testNullableValue(t, NullableTimestamp64Value, TypeTimestamp64, int64(-1), Timestamp64Value)
	testNullableValue(t, NullableIntervalValueFromMicroseconds, TypeInterval, int64(-1234), IntervalValueFromMicroseconds)
	testNullableValue(t, NullableIntervalValueFromDuration, TypeInterval, time.Second, IntervalValueFromDuration)
	testNullableValue(t, NullableInterval64ValueFromNanoseconds, TypeInterval64, int64(-1234),
		Interval64ValueFromNanoseconds,
	)
	testNullableValue(t, NullableInterval64ValueFromDuration, TypeInterval64, time.Second, func(v time.Duration) Value {
		return value.Interval64Value(v.Microseconds())
	})
	testNullableValue(t, NullableBytesValue, TypeBytes, []byte{0, 1, 255}, BytesValue)
	testNullableValue(t, NullableBytesValueFromString, TypeBytes, "bytes", BytesValueFromString)
	testNullableValue(t, NullableStringValueFromString, TypeBytes, "bytes", StringValueFromString)
	testNullableValue(t, NullableTextValue, TypeText, "text", TextValue)
	testNullableValue(t, NullableUTF8Value, TypeText, "text", UTF8Value)
	testNullableValue(t, NullableYSONValue, TypeYSON, "{}", YSONValue)
	testNullableValue(t, NullableYSONValueFromBytes, TypeYSON, []byte("{}"), YSONValueFromBytes)
	testNullableValue(t, NullableJSONValue, TypeJSON, "{}", JSONValue)
	testNullableValue(t, NullableJSONValueFromBytes, TypeJSON, []byte("{}"), JSONValueFromBytes)
	testNullableValue(t, NullableJSONDocumentValue, TypeJSONDocument, "{}", JSONDocumentValue)
	testNullableValue(t, NullableJSONDocumentValueFromBytes, TypeJSONDocument, []byte("{}"), JSONDocumentValueFromBytes)
	testNullableValue(t, NullableDyNumberValue, TypeDyNumber, "1.25", DyNumberValue)
	testNullableValue(t, NullableUUIDValue, TypeUUID, [16]byte{1, 2, 3, 4}, UUIDWithIssue1501Value)
	testNullableValue(t, NullableUUIDValueWithIssue1501, TypeUUID, [16]byte{1, 2, 3, 4}, UUIDWithIssue1501Value)
	testNullableValue(t, NullableUUIDTypedValue, TypeUUID,
		uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"), UuidValue,
	)
}

func testNullableValue[T any](t *testing.T, nullable func(*T) Value, typ Type, input T, scalar func(T) Value) {
	t.Helper()

	t.Run(typ.Yql(), func(t *testing.T) {
		t.Run("Nil", func(t *testing.T) {
			got := nullable(nil)
			require.True(t, IsNull(got))
			require.True(t, Equal(Optional(typ), got.Type()))
			require.True(t, proto.Equal(value.ToYDB(NullValue(typ)), value.ToYDB(got)))
		})
		for _, input := range []T{input, *new(T)} {
			t.Run("Value", func(t *testing.T) {
				got := nullable(&input)
				require.False(t, IsNull(got))
				require.True(t, Equal(Optional(typ), got.Type()))
				want := OptionalValue(scalar(input))
				require.Equal(t, want.Yql(), got.Yql())
				require.True(t, proto.Equal(value.ToYDB(want), value.ToYDB(got)))
			})
		}
	})
}
