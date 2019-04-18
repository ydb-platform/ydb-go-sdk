package ydb

import (
	"github.com/yandex-cloud/ydb-go-sdk/internal"
)

type Value interface {
	internal.V
}

func BoolValue(v bool) Value          { return internal.BoolValue(v) }
func Int8Value(v int8) Value          { return internal.Int8Value(v) }
func Uint8Value(v uint8) Value        { return internal.Uint8Value(v) }
func Int16Value(v int16) Value        { return internal.Int16Value(v) }
func Uint16Value(v uint16) Value      { return internal.Uint16Value(v) }
func Int32Value(v int32) Value        { return internal.Int32Value(v) }
func Uint32Value(v uint32) Value      { return internal.Uint32Value(v) }
func Int64Value(v int64) Value        { return internal.Int64Value(v) }
func Uint64Value(v uint64) Value      { return internal.Uint64Value(v) }
func FloatValue(v float32) Value      { return internal.FloatValue(v) }
func DoubleValue(v float64) Value     { return internal.DoubleValue(v) }
func DateValue(v uint32) Value        { return internal.DateValue(v) }
func DatetimeValue(v uint32) Value    { return internal.DatetimeValue(v) }
func TimestampValue(v uint64) Value   { return internal.TimestampValue(v) }
func IntervalValue(v int64) Value     { return internal.IntervalValue(v) }
func TzDateValue(v string) Value      { return internal.TzDateValue(v) }
func TzDatetimeValue(v string) Value  { return internal.TzDatetimeValue(v) }
func TzTimestampValue(v string) Value { return internal.TzTimestampValue(v) }
func StringValue(v []byte) Value      { return internal.StringValue(v) }
func UTF8Value(v string) Value        { return internal.UTF8Value(v) }
func YSONValue(v string) Value        { return internal.YSONValue(v) }
func JSONValue(v string) Value        { return internal.JSONValue(v) }
func UUIDValue(v [16]byte) Value      { return internal.UUIDValue(v) }

func VoidValue() Value            { return internal.VoidValue }
func NullValue(t Type) Value      { return internal.NullValue(t) }
func OptionalValue(v Value) Value { return internal.OptionalValue(v) }

// DecimalValue creates decimal value of given type t and value v.
// Note that v interpreted as big-endian int128.
func DecimalValue(t Type, v [16]byte) Value {
	return internal.DecimalValue(t, v)
}

func TupleValue(vs ...Value) Value {
	return internal.TupleValue(len(vs), func(i int) internal.V {
		return vs[i]
	})
}

func ListValue(vs ...Value) Value {
	return internal.ListValue(len(vs), func(i int) internal.V {
		return vs[i]
	})
}

type tStructValueProto internal.StructValueProto

type StructValueOption func(*tStructValueProto)

func StructFieldValue(name string, value Value) StructValueOption {
	return func(p *tStructValueProto) {
		(*internal.StructValueProto)(p).Add(name, value)
	}
}

func StructValue(opts ...StructValueOption) Value {
	var p tStructValueProto
	for _, opt := range opts {
		opt(&p)
	}
	return internal.StructValue((*internal.StructValueProto)(&p))
}

func DictValue(pairs ...Value) Value {
	return internal.DictValue(len(pairs), func(i int) internal.V {
		return pairs[i]
	})
}

func VariantValue(v Value, i uint32, variantT Type) Value {
	return internal.VariantValue(v, i, variantT)
}
