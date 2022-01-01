package types

import (
	"math/big"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/decimal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type Value interface {
	value.V
}

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

func DateValue(v uint32) Value { return value.DateValue(v) }

func DatetimeValue(v uint32) Value { return value.DatetimeValue(v) }

func TimestampValue(v uint64) Value { return value.TimestampValue(v) }

func IntervalValue(v int64) Value { return value.IntervalValue(v) }

func TzDateValue(v string) Value { return value.TzDateValue(v) }

func TzDatetimeValue(v string) Value { return value.TzDatetimeValue(v) }

func TzTimestampValue(v string) Value { return value.TzTimestampValue(v) }

func DateValueFromTime(v time.Time) Value { return value.DateValue(timeutil.MarshalDate(v)) }

func DatetimeValueFromTime(v time.Time) Value {
	return value.DatetimeValue(timeutil.MarshalDatetime(v))
}

func TimestampValueFromTime(v time.Time) Value {
	return value.TimestampValue(timeutil.MarshalTimestamp(v))
}

func IntervalValueFromDuration(v time.Duration) Value {
	return value.IntervalValue(timeutil.MarshalInterval(v))
}

func TzDateValueFromTime(v time.Time) Value { return value.TzDateValue(timeutil.MarshalTzDate(v)) }

func TzDatetimeValueFromTime(v time.Time) Value {
	return value.TzDatetimeValue(timeutil.MarshalTzDatetime(v))
}

func TzTimestampValueFromTime(v time.Time) Value {
	return value.TzTimestampValue(timeutil.MarshalTzTimestamp(v))
}

func StringValue(v []byte) Value { return value.StringValue(v) }

func StringValueFromString(v string) Value { return value.StringValue([]byte(v)) }

func UTF8Value(v string) Value { return value.UTF8Value(v) }

func YSONValue(v string) Value { return value.YSONValue(v) }

func YSONValueFromBytes(v []byte) Value { return value.YSONValue(string(v)) }

func JSONValue(v string) Value { return value.JSONValue(v) }

func JSONValueFromBytes(v []byte) Value { return value.JSONValue(string(v)) }

func UUIDValue(v [16]byte) Value { return value.UUIDValue(v) }

func JSONDocumentValue(v string) Value { return value.JSONDocumentValue(v) }

func JSONDocumentValueFromBytes(v []byte) Value { return value.JSONDocumentValue(string(v)) }

func DyNumberValue(v string) Value { return value.DyNumberValue(v) }

func VoidValue() Value { return value.VoidValue }

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
	t := DecimalTypeFromDecimal(v)
	return value.DecimalValue(t, v.Bytes)
}

func DecimalValueFromBigInt(v *big.Int, precision, scale uint32) Value {
	b := decimal.BigIntToByte(v, precision, scale)
	t := DecimalType(precision, scale)
	return value.DecimalValue(t, b)
}

func TupleValue(vs ...Value) Value {
	return value.TupleValue(len(vs), func(i int) value.V {
		return vs[i]
	})
}

func ListValue(vs ...Value) Value {
	return value.ListValue(len(vs), func(i int) value.V {
		return vs[i]
	})
}

type tStructValueProto value.StructValueProto

type StructValueOption func(*tStructValueProto)

func StructFieldValue(name string, v Value) StructValueOption {
	return func(p *tStructValueProto) {
		(*value.StructValueProto)(p).Add(name, v)
	}
}

func StructValue(opts ...StructValueOption) Value {
	var p tStructValueProto
	for _, opt := range opts {
		opt(&p)
	}
	return value.StructValue((*value.StructValueProto)(&p))
}

func DictValue(pairs ...Value) Value {
	return value.DictValue(len(pairs), func(i int) value.V {
		return pairs[i]
	})
}

func VariantValue(v Value, i uint32, variantT Type) Value {
	return value.VariantValue(v, i, variantT)
}
