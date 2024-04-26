package value

import (
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

func NullableBoolValue(v *bool) Value {
	if v == nil {
		return NullValue(types.Bool)
	}

	return OptionalValue(BoolValue(*v))
}

func NullableInt8Value(v *int8) Value {
	if v == nil {
		return NullValue(types.Int8)
	}

	return OptionalValue(Int8Value(*v))
}

func NullableInt16Value(v *int16) Value {
	if v == nil {
		return NullValue(types.Int16)
	}

	return OptionalValue(Int16Value(*v))
}

func NullableInt32Value(v *int32) Value {
	if v == nil {
		return NullValue(types.Int32)
	}

	return OptionalValue(Int32Value(*v))
}

func NullableInt64Value(v *int64) Value {
	if v == nil {
		return NullValue(types.Int64)
	}

	return OptionalValue(Int64Value(*v))
}

func NullableUint8Value(v *uint8) Value {
	if v == nil {
		return NullValue(types.Uint8)
	}

	return OptionalValue(Uint8Value(*v))
}

func NullableUint16Value(v *uint16) Value {
	if v == nil {
		return NullValue(types.Uint16)
	}

	return OptionalValue(Uint16Value(*v))
}

func NullableUint32Value(v *uint32) Value {
	if v == nil {
		return NullValue(types.Uint32)
	}

	return OptionalValue(Uint32Value(*v))
}

func NullableUint64Value(v *uint64) Value {
	if v == nil {
		return NullValue(types.Uint64)
	}

	return OptionalValue(Uint64Value(*v))
}

func NullableFloatValue(v *float32) Value {
	if v == nil {
		return NullValue(types.Float)
	}

	return OptionalValue(FloatValue(*v))
}

func NullableDoubleValue(v *float64) Value {
	if v == nil {
		return NullValue(types.Double)
	}

	return OptionalValue(DoubleValue(*v))
}

func NullableDateValue(v *uint32) Value {
	if v == nil {
		return NullValue(types.Date)
	}

	return OptionalValue(DateValue(*v))
}

func NullableDateValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(types.Date)
	}

	return OptionalValue(DateValueFromTime(*v))
}

func NullableDatetimeValue(v *uint32) Value {
	if v == nil {
		return NullValue(types.Datetime)
	}

	return OptionalValue(DatetimeValue(*v))
}

func NullableDatetimeValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(types.Datetime)
	}

	return OptionalValue(DatetimeValueFromTime(*v))
}

func NullableTzDateValue(v *string) Value {
	if v == nil {
		return NullValue(types.TzDate)
	}

	return OptionalValue(TzDateValue(*v))
}

func NullableTzDateValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(types.TzDate)
	}

	return OptionalValue(TzDateValueFromTime(*v))
}

func NullableTzDatetimeValue(v *string) Value {
	if v == nil {
		return NullValue(types.TzDatetime)
	}

	return OptionalValue(TzDatetimeValue(*v))
}

func NullableTzDatetimeValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(types.TzDatetime)
	}

	return OptionalValue(TzDatetimeValueFromTime(*v))
}

func NullableTimestampValue(v *uint64) Value {
	if v == nil {
		return NullValue(types.Timestamp)
	}

	return OptionalValue(TimestampValue(*v))
}

func NullableTimestampValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(types.Timestamp)
	}

	return OptionalValue(TimestampValueFromTime(*v))
}

func NullableTzTimestampValue(v *string) Value {
	if v == nil {
		return NullValue(types.TzTimestamp)
	}

	return OptionalValue(TzTimestampValue(*v))
}

func NullableTzTimestampValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(types.TzTimestamp)
	}

	return OptionalValue(TzTimestampValueFromTime(*v))
}

func NullableIntervalValueFromMicroseconds(v *int64) Value {
	if v == nil {
		return NullValue(types.Interval)
	}

	return OptionalValue(IntervalValue(*v))
}

func NullableIntervalValueFromDuration(v *time.Duration) Value {
	if v == nil {
		return NullValue(types.Interval)
	}

	return OptionalValue(IntervalValueFromDuration(*v))
}

func NullableBytesValue(v *[]byte) Value {
	if v == nil {
		return NullValue(types.Bytes)
	}

	return OptionalValue(BytesValue(*v))
}

func NullableBytesValueFromString(v *string) Value {
	if v == nil {
		return NullValue(types.Bytes)
	}

	return OptionalValue(BytesValue(xstring.ToBytes(*v)))
}

func NullableTextValue(v *string) Value {
	if v == nil {
		return NullValue(types.Text)
	}

	return OptionalValue(TextValue(*v))
}

func NullableYSONValue(v *string) Value {
	if v == nil {
		return NullValue(types.YSON)
	}

	return OptionalValue(YSONValue(xstring.ToBytes(*v)))
}

func NullableYSONValueFromBytes(v *[]byte) Value {
	if v == nil {
		return NullValue(types.YSON)
	}

	return OptionalValue(YSONValue(*v))
}

func NullableJSONValue(v *string) Value {
	if v == nil {
		return NullValue(types.JSON)
	}

	return OptionalValue(JSONValue(*v))
}

func NullableJSONValueFromBytes(v *[]byte) Value {
	if v == nil {
		return NullValue(types.JSON)
	}

	return OptionalValue(JSONValue(xstring.FromBytes(*v)))
}

func NullableUUIDValue(v *[16]byte) Value {
	if v == nil {
		return NullValue(types.UUID)
	}

	return OptionalValue(UUIDValue(*v))
}

func NullableJSONDocumentValue(v *string) Value {
	if v == nil {
		return NullValue(types.JSONDocument)
	}

	return OptionalValue(JSONDocumentValue(*v))
}

func NullableJSONDocumentValueFromBytes(v *[]byte) Value {
	if v == nil {
		return NullValue(types.JSONDocument)
	}

	return OptionalValue(JSONDocumentValue(xstring.FromBytes(*v)))
}

func NullableDyNumberValue(v *string) Value {
	if v == nil {
		return NullValue(types.DyNumber)
	}

	return OptionalValue(DyNumberValue(*v))
}

// Nullable makes optional value from nullable type
// Warning: type interface will be replaced in the future with typed parameters pattern from go1.18
//
//nolint:gocyclo
func Nullable(t types.Type, v interface{}) Value {
	switch t {
	case types.Bool:
		return NullableBoolValue(v.(*bool))
	case types.Int8:
		return NullableInt8Value(v.(*int8))
	case types.Uint8:
		return NullableUint8Value(v.(*uint8))
	case types.Int16:
		return NullableInt16Value(v.(*int16))
	case types.Uint16:
		return NullableUint16Value(v.(*uint16))
	case types.Int32:
		return NullableInt32Value(v.(*int32))
	case types.Uint32:
		return NullableUint32Value(v.(*uint32))
	case types.Int64:
		return NullableInt64Value(v.(*int64))
	case types.Uint64:
		return NullableUint64Value(v.(*uint64))
	case types.Float:
		return NullableFloatValue(v.(*float32))
	case types.Double:
		return NullableDoubleValue(v.(*float64))
	case types.Date:
		switch tt := v.(type) {
		case *uint32:
			return NullableDateValue(tt)
		case *time.Time:
			return NullableDateValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeDate", tt))
		}
	case types.Datetime:
		switch tt := v.(type) {
		case *uint32:
			return NullableDatetimeValue(tt)
		case *time.Time:
			return NullableDatetimeValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeDatetime", tt))
		}
	case types.Timestamp:
		switch tt := v.(type) {
		case *uint64:
			return NullableTimestampValue(tt)
		case *time.Time:
			return NullableTimestampValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeTimestamp", tt))
		}
	case types.Interval:
		switch tt := v.(type) {
		case *int64:
			return NullableIntervalValueFromMicroseconds(tt)
		case *time.Duration:
			return NullableIntervalValueFromDuration(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeInterval", tt))
		}
	case types.TzDate:
		switch tt := v.(type) {
		case *string:
			return NullableTzDateValue(tt)
		case *time.Time:
			return NullableTzDateValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeTzDate", tt))
		}
	case types.TzDatetime:
		switch tt := v.(type) {
		case *string:
			return NullableTzDatetimeValue(tt)
		case *time.Time:
			return NullableTzDatetimeValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeTzDatetime", tt))
		}
	case types.TzTimestamp:
		switch tt := v.(type) {
		case *string:
			return NullableTzTimestampValue(tt)
		case *time.Time:
			return NullableTzTimestampValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeTzTimestamp", tt))
		}
	case types.Bytes:
		switch tt := v.(type) {
		case *[]byte:
			return NullableBytesValue(tt)
		case *string:
			return NullableBytesValueFromString(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeBytes", tt))
		}
	case types.Text:
		switch tt := v.(type) {
		case *string:
			return NullableTextValue(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeText", tt))
		}
	case types.YSON:
		switch tt := v.(type) {
		case *string:
			return NullableYSONValue(tt)
		case *[]byte:
			return NullableYSONValueFromBytes(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeYSON", tt))
		}
	case types.JSON:
		switch tt := v.(type) {
		case *string:
			return NullableJSONValue(tt)
		case *[]byte:
			return NullableJSONValueFromBytes(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeJSON", tt))
		}
	case types.UUID:
		switch tt := v.(type) {
		case *[16]byte:
			return NullableUUIDValue(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeUUID", tt))
		}
	case types.JSONDocument:
		switch tt := v.(type) {
		case *string:
			return NullableJSONDocumentValue(tt)
		case *[]byte:
			return NullableJSONDocumentValueFromBytes(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeJSONDocument", tt))
		}
	case types.DyNumber:
		switch tt := v.(type) {
		case *string:
			return NullableDyNumberValue(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeDyNumber", tt))
		}
	default:
		panic(fmt.Sprintf("unsupported type: %T", t))
	}
}
