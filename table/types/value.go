package types

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/decimal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type Value = value.Value

func BoolValue(v bool) Value { return value.BoolValue(v) }

func Int8Value(v int8) Value { return value.Int8Value(v) }

func Uint8Value(v uint8) Value { return value.Uint8Value(v) }

func Int16Value(v int16) Value { return value.Int16Value(v) }

func Uint16Value(v uint16) Value { return value.Uint16Value(v) }

func Int32Value(v int32) Value { return value.Int32Value(v) }

func Uint32Value(v uint32) Value { return value.Uint32Value(v) }

func Int64Value(v int64) Value { return value.Int64Value(v) }

func Uint64Value(v uint64) Value { return value.Uint64Value(v) }

func FloatValue(v float32) Value { return value.FloatValue(v) }

func DoubleValue(v float64) Value { return value.DoubleValue(v) }

// DateValue returns ydb date value by given days since Epoch
func DateValue(v uint32) Value { return value.DateValue(v) }

// DatetimeValue makes ydb datetime value from seconds since Epoch
func DatetimeValue(v uint32) Value { return value.DatetimeValue(v) }

// TimestampValue makes ydb timestamp value from microseconds since Epoch
func TimestampValue(v uint64) Value { return value.TimestampValue(v) }

// IntervalValueFromMicroseconds makes Value from given microseconds value
func IntervalValueFromMicroseconds(v int64) Value { return value.IntervalValue(v) }

// IntervalValue makes Value from given microseconds value
//
// Deprecated: use IntervalValueFromMicroseconds instead
func IntervalValue(v int64) Value { return value.IntervalValue(v) }

// TzDateValue makes TzDate value from string
func TzDateValue(v string) Value { return value.TzDateValue(v) }

// TzDatetimeValue makes TzDatetime value from string
func TzDatetimeValue(v string) Value { return value.TzDatetimeValue(v) }

// TzTimestampValue makes TzTimestamp value from string
func TzTimestampValue(v string) Value { return value.TzTimestampValue(v) }

// DateValueFromTime makes Date value from time.Time
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func DateValueFromTime(t time.Time) Value {
	return value.DateValueFromTime(t)
}

// DatetimeValueFromTime makes Datetime value from time.Time
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func DatetimeValueFromTime(t time.Time) Value {
	return value.DatetimeValueFromTime(t)
}

// TimestampValueFromTime makes Timestamp value from time.Time
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func TimestampValueFromTime(t time.Time) Value {
	return value.TimestampValueFromTime(t)
}

// IntervalValueFromDuration makes Interval value from time.Duration
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func IntervalValueFromDuration(v time.Duration) Value {
	return value.IntervalValueFromDuration(v)
}

// TzDateValueFromTime makes TzDate value from time.Time
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func TzDateValueFromTime(t time.Time) Value {
	return value.TzDateValueFromTime(t)
}

// TzDatetimeValueFromTime makes TzDatetime value from time.Time
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func TzDatetimeValueFromTime(t time.Time) Value {
	return value.TzDatetimeValueFromTime(t)
}

// TzTimestampValueFromTime makes TzTimestamp value from time.Time
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func TzTimestampValueFromTime(t time.Time) Value {
	return value.TzTimestampValueFromTime(t)
}

// StringValue returns bytes value
//
// Deprecated: use BytesValue instead
func StringValue(v []byte) Value { return value.BytesValue(v) }

func BytesValue(v []byte) Value { return value.BytesValue(v) }

func BytesValueFromString(v string) Value { return value.BytesValue(xstring.ToBytes(v)) }

// StringValueFromString makes String value from string
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func StringValueFromString(v string) Value { return value.BytesValue(xstring.ToBytes(v)) }

func UTF8Value(v string) Value { return value.TextValue(v) }

func TextValue(v string) Value { return value.TextValue(v) }

func YSONValue(v string) Value { return value.YSONValue(xstring.ToBytes(v)) }

// YSONValueFromBytes makes YSON value from bytes
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func YSONValueFromBytes(v []byte) Value { return value.YSONValue(v) }

func JSONValue(v string) Value { return value.JSONValue(v) }

// JSONValueFromBytes makes JSON value from bytes
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func JSONValueFromBytes(v []byte) Value { return value.JSONValue(xstring.FromBytes(v)) }

func UUIDValue(v [16]byte) Value { return value.UUIDValue(v) }

func JSONDocumentValue(v string) Value { return value.JSONDocumentValue(v) }

// JSONDocumentValueFromBytes makes JSONDocument value from bytes
//
// Warning: all *From* helpers will be removed at next major release
// (functional will be implements with go1.18 type lists)
func JSONDocumentValueFromBytes(v []byte) Value {
	return value.JSONDocumentValue(xstring.FromBytes(v))
}

func DyNumberValue(v string) Value { return value.DyNumberValue(v) }

func VoidValue() Value { return value.VoidValue() }

func NullValue(t Type) Value { return value.NullValue(t) }

func ZeroValue(t Type) Value { return value.ZeroValue(t) }

func OptionalValue(v Value) Value { return value.OptionalValue(v) }

// Decimal supported in scanner API
type Decimal struct {
	Bytes     [16]byte
	Precision uint32
	Scale     uint32
}

func (d *Decimal) String() string {
	v := decimal.FromInt128(d.Bytes, d.Precision, d.Scale)
	return decimal.Format(v, d.Precision, d.Scale)
}

func (d *Decimal) BigInt() *big.Int {
	return decimal.FromInt128(d.Bytes, d.Precision, d.Scale)
}

// DecimalValue creates decimal value of given types t and value v.
// Note that Decimal.Bytes interpreted as big-endian int128.
func DecimalValue(v *Decimal) Value {
	return value.DecimalValue(v.Bytes, v.Precision, v.Scale)
}

func DecimalValueFromBigInt(v *big.Int, precision, scale uint32) Value {
	return value.DecimalValueFromBigInt(v, precision, scale)
}

func TupleValue(vs ...Value) Value {
	return value.TupleValue(vs...)
}

func ListValue(vs ...Value) Value {
	return value.ListValue(vs...)
}

func SetValue(vs ...Value) Value {
	return value.SetValue(vs...)
}

type structValueFields struct {
	fields []value.StructValueField
}

type StructValueOption func(*structValueFields)

func StructFieldValue(name string, v Value) StructValueOption {
	return func(t *structValueFields) {
		t.fields = append(t.fields, value.StructValueField{Name: name, V: v})
	}
}

func StructValue(opts ...StructValueOption) Value {
	var p structValueFields
	for _, opt := range opts {
		if opt != nil {
			opt(&p)
		}
	}
	return value.StructValue(p.fields...)
}

type dictValueFields struct {
	fields []value.DictValueField
}

type DictValueOption func(*dictValueFields)

func DictFieldValue(k, v Value) DictValueOption {
	return func(t *dictValueFields) {
		t.fields = append(t.fields, value.DictValueField{K: k, V: v})
	}
}

func DictValue(opts ...DictValueOption) Value {
	var p dictValueFields
	for _, opt := range opts {
		if opt != nil {
			opt(&p)
		}
	}
	return value.DictValue(p.fields...)
}

func VariantValueStruct(v Value, name string, variantT Type) Value {
	return value.VariantValueStruct(v, name, variantT)
}

func VariantValueTuple(v Value, i uint32, variantT Type) Value {
	return value.VariantValueTuple(v, i, variantT)
}

func NullableBoolValue(v *bool) Value {
	if v == nil {
		return NullValue(TypeBool)
	}
	return OptionalValue(BoolValue(*v))
}

func NullableInt8Value(v *int8) Value {
	if v == nil {
		return NullValue(TypeInt8)
	}
	return OptionalValue(Int8Value(*v))
}

func NullableInt16Value(v *int16) Value {
	if v == nil {
		return NullValue(TypeInt16)
	}
	return OptionalValue(Int16Value(*v))
}

func NullableInt32Value(v *int32) Value {
	if v == nil {
		return NullValue(TypeInt32)
	}
	return OptionalValue(Int32Value(*v))
}

func NullableInt64Value(v *int64) Value {
	if v == nil {
		return NullValue(TypeInt64)
	}
	return OptionalValue(Int64Value(*v))
}

func NullableUint8Value(v *uint8) Value {
	if v == nil {
		return NullValue(TypeUint8)
	}
	return OptionalValue(Uint8Value(*v))
}

func NullableUint16Value(v *uint16) Value {
	if v == nil {
		return NullValue(TypeUint16)
	}
	return OptionalValue(Uint16Value(*v))
}

func NullableUint32Value(v *uint32) Value {
	if v == nil {
		return NullValue(TypeUint32)
	}
	return OptionalValue(Uint32Value(*v))
}

func NullableUint64Value(v *uint64) Value {
	if v == nil {
		return NullValue(TypeUint64)
	}
	return OptionalValue(Uint64Value(*v))
}

func NullableFloatValue(v *float32) Value {
	if v == nil {
		return NullValue(TypeFloat)
	}
	return OptionalValue(FloatValue(*v))
}

func NullableDoubleValue(v *float64) Value {
	if v == nil {
		return NullValue(TypeDouble)
	}
	return OptionalValue(DoubleValue(*v))
}

func NullableDateValue(v *uint32) Value {
	if v == nil {
		return NullValue(TypeDate)
	}
	return OptionalValue(DateValue(*v))
}

func NullableDateValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(TypeDate)
	}
	return OptionalValue(DateValueFromTime(*v))
}

func NullableDatetimeValue(v *uint32) Value {
	if v == nil {
		return NullValue(TypeDatetime)
	}
	return OptionalValue(DatetimeValue(*v))
}

func NullableDatetimeValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(TypeDatetime)
	}
	return OptionalValue(DatetimeValueFromTime(*v))
}

func NullableTzDateValue(v *string) Value {
	if v == nil {
		return NullValue(TypeTzDate)
	}
	return OptionalValue(TzDateValue(*v))
}

func NullableTzDateValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(TypeTzDate)
	}
	return OptionalValue(TzDateValueFromTime(*v))
}

func NullableTzDatetimeValue(v *string) Value {
	if v == nil {
		return NullValue(TypeTzDatetime)
	}
	return OptionalValue(TzDatetimeValue(*v))
}

func NullableTzDatetimeValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(TypeTzDatetime)
	}
	return OptionalValue(TzDatetimeValueFromTime(*v))
}

func NullableTimestampValue(v *uint64) Value {
	if v == nil {
		return NullValue(TypeTimestamp)
	}
	return OptionalValue(TimestampValue(*v))
}

func NullableTimestampValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(TypeTimestamp)
	}
	return OptionalValue(TimestampValueFromTime(*v))
}

func NullableTzTimestampValue(v *string) Value {
	if v == nil {
		return NullValue(TypeTzTimestamp)
	}
	return OptionalValue(TzTimestampValue(*v))
}

func NullableTzTimestampValueFromTime(v *time.Time) Value {
	if v == nil {
		return NullValue(TypeTzTimestamp)
	}
	return OptionalValue(TzTimestampValueFromTime(*v))
}

// NullableIntervalValue makes Value which maybe nil or valued
//
// Deprecated: use NullableIntervalValueFromMicroseconds instead
func NullableIntervalValue(v *int64) Value {
	if v == nil {
		return NullValue(TypeInterval)
	}
	return OptionalValue(IntervalValue(*v))
}

func NullableIntervalValueFromMicroseconds(v *int64) Value {
	if v == nil {
		return NullValue(TypeInterval)
	}
	return OptionalValue(IntervalValueFromMicroseconds(*v))
}

func NullableIntervalValueFromDuration(v *time.Duration) Value {
	if v == nil {
		return NullValue(TypeInterval)
	}
	return OptionalValue(IntervalValueFromDuration(*v))
}

// NullableStringValue
//
// Deprecated: use NullableBytesValue instead
func NullableStringValue(v *[]byte) Value {
	if v == nil {
		return NullValue(TypeBytes)
	}
	return OptionalValue(StringValue(*v))
}

func NullableBytesValue(v *[]byte) Value {
	if v == nil {
		return NullValue(TypeBytes)
	}
	return OptionalValue(BytesValue(*v))
}

func NullableStringValueFromString(v *string) Value {
	if v == nil {
		return NullValue(TypeBytes)
	}
	return OptionalValue(BytesValueFromString(*v))
}

func NullableBytesValueFromString(v *string) Value {
	if v == nil {
		return NullValue(TypeBytes)
	}
	return OptionalValue(BytesValueFromString(*v))
}

func NullableUTF8Value(v *string) Value {
	if v == nil {
		return NullValue(TypeText)
	}
	return OptionalValue(TextValue(*v))
}

func NullableTextValue(v *string) Value {
	if v == nil {
		return NullValue(TypeText)
	}
	return OptionalValue(TextValue(*v))
}

func NullableYSONValue(v *string) Value {
	if v == nil {
		return NullValue(TypeYSON)
	}
	return OptionalValue(YSONValue(*v))
}

func NullableYSONValueFromBytes(v *[]byte) Value {
	if v == nil {
		return NullValue(TypeYSON)
	}
	return OptionalValue(YSONValueFromBytes(*v))
}

func NullableJSONValue(v *string) Value {
	if v == nil {
		return NullValue(TypeJSON)
	}
	return OptionalValue(JSONValue(*v))
}

func NullableJSONValueFromBytes(v *[]byte) Value {
	if v == nil {
		return NullValue(TypeJSON)
	}
	return OptionalValue(JSONValueFromBytes(*v))
}

func NullableUUIDValue(v *[16]byte) Value {
	if v == nil {
		return NullValue(TypeUUID)
	}
	return OptionalValue(UUIDValue(*v))
}

func NullableJSONDocumentValue(v *string) Value {
	if v == nil {
		return NullValue(TypeJSONDocument)
	}
	return OptionalValue(JSONDocumentValue(*v))
}

func NullableJSONDocumentValueFromBytes(v *[]byte) Value {
	if v == nil {
		return NullValue(TypeJSONDocument)
	}
	return OptionalValue(JSONDocumentValueFromBytes(*v))
}

func NullableDyNumberValue(v *string) Value {
	if v == nil {
		return NullValue(TypeDyNumber)
	}
	return OptionalValue(DyNumberValue(*v))
}

// Nullable makes optional value from nullable type
// Warning: type interface will be replaced in the future with typed parameters pattern from go1.18
//
//nolint:gocyclo
func Nullable(t Type, v interface{}) Value {
	switch t {
	case TypeBool:
		return NullableBoolValue(v.(*bool))
	case TypeInt8:
		return NullableInt8Value(v.(*int8))
	case TypeUint8:
		return NullableUint8Value(v.(*uint8))
	case TypeInt16:
		return NullableInt16Value(v.(*int16))
	case TypeUint16:
		return NullableUint16Value(v.(*uint16))
	case TypeInt32:
		return NullableInt32Value(v.(*int32))
	case TypeUint32:
		return NullableUint32Value(v.(*uint32))
	case TypeInt64:
		return NullableInt64Value(v.(*int64))
	case TypeUint64:
		return NullableUint64Value(v.(*uint64))
	case TypeFloat:
		return NullableFloatValue(v.(*float32))
	case TypeDouble:
		return NullableDoubleValue(v.(*float64))
	case TypeDate:
		switch tt := v.(type) {
		case *uint32:
			return NullableDateValue(tt)
		case *time.Time:
			return NullableDateValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeDate", tt))
		}
	case TypeDatetime:
		switch tt := v.(type) {
		case *uint32:
			return NullableDatetimeValue(tt)
		case *time.Time:
			return NullableDatetimeValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeDatetime", tt))
		}
	case TypeTimestamp:
		switch tt := v.(type) {
		case *uint64:
			return NullableTimestampValue(tt)
		case *time.Time:
			return NullableTimestampValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeTimestamp", tt))
		}
	case TypeInterval:
		switch tt := v.(type) {
		case *int64:
			return NullableIntervalValueFromMicroseconds(tt)
		case *time.Duration:
			return NullableIntervalValueFromDuration(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeInterval", tt))
		}
	case TypeTzDate:
		switch tt := v.(type) {
		case *string:
			return NullableTzDateValue(tt)
		case *time.Time:
			return NullableTzDateValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeTzDate", tt))
		}
	case TypeTzDatetime:
		switch tt := v.(type) {
		case *string:
			return NullableTzDatetimeValue(tt)
		case *time.Time:
			return NullableTzDatetimeValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeTzDatetime", tt))
		}
	case TypeTzTimestamp:
		switch tt := v.(type) {
		case *string:
			return NullableTzTimestampValue(tt)
		case *time.Time:
			return NullableTzTimestampValueFromTime(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeTzTimestamp", tt))
		}
	case TypeBytes:
		switch tt := v.(type) {
		case *[]byte:
			return NullableBytesValue(tt)
		case *string:
			return NullableStringValueFromString(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeBytes", tt))
		}
	case TypeText:
		switch tt := v.(type) {
		case *string:
			return NullableTextValue(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeText", tt))
		}
	case TypeYSON:
		switch tt := v.(type) {
		case *string:
			return NullableYSONValue(tt)
		case *[]byte:
			return NullableYSONValueFromBytes(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeYSON", tt))
		}
	case TypeJSON:
		switch tt := v.(type) {
		case *string:
			return NullableJSONValue(tt)
		case *[]byte:
			return NullableJSONValueFromBytes(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeJSON", tt))
		}
	case TypeUUID:
		switch tt := v.(type) {
		case *[16]byte:
			return NullableUUIDValue(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeUUID", tt))
		}
	case TypeJSONDocument:
		switch tt := v.(type) {
		case *string:
			return NullableJSONDocumentValue(tt)
		case *[]byte:
			return NullableJSONDocumentValueFromBytes(tt)
		default:
			panic(fmt.Sprintf("unsupported type conversion from %T to TypeJSONDocument", tt))
		}
	case TypeDyNumber:
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
