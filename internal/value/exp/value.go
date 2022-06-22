package value

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type V interface {
	getType() T
	toYDBType(a *allocator.Allocator) *Ydb.Type
	toYDBValue(a *allocator.Allocator) *Ydb.Value
	toString(*bytes.Buffer)

	String() string
}

func ToYDB(v V, a *allocator.Allocator) *Ydb.TypedValue {
	tv := a.TypedValue()

	tv.Type = v.toYDBType(a)
	tv.Value = v.toYDBValue(a)

	return tv
}

func valueToString(buf *bytes.Buffer, t T, v *Ydb.Value) {
	buf.WriteByte('(')
	defer buf.WriteByte(')')
	if x, ok := primitiveFromYDB(v); ok {
		if x != nil {
			fmt.Fprintf(buf, "%v", x)
		} else {
			buf.WriteString("NULL")
		}
		return
	}
	if x, ok := v.Value.(*Ydb.Value_NestedValue); ok {
		switch x := t.(type) {
		case *variantType:
			var (
				i = int(v.VariantIndex)
				s string
			)
			switch x.tt {
			case variantTypeTuple:
				t = x.t.(*TupleType).items[i]
				s = strconv.Itoa(i)
			case variantTypeStruct:
				f := x.t.(*StructType).fields[i]
				t = f.T
				s = f.Name
			}
			buf.WriteString(s)
			buf.WriteByte('=')

		case *optionalType:
			t = x.t

		default:
			panic("ydb: unknown nested types")
		}
		valueToString(buf, t, x.NestedValue)
		return
	}
	if n := len(v.Items); n > 0 {
		types := make([]T, n)
		switch x := t.(type) {
		case *StructType:
			for i, f := range x.fields {
				types[i] = f.T
			}
		case *listType:
			for i := range types {
				types[i] = x.t
			}
		case *TupleType:
			copy(types, x.items)
		default:
			panic(fmt.Sprintf("ydb: unknown iterable types: %v", x))
		}
		for i, item := range v.Items {
			valueToString(buf, types[i], item)
		}
		return
	}
	if len(v.Pairs) > 0 {
		dict := t.(*dictType)
		for _, pair := range v.Pairs {
			buf.WriteByte('(')
			valueToString(buf, dict.k, pair.Key)
			valueToString(buf, dict.v, pair.Payload)
			buf.WriteByte(')')
		}
	}
}

func primitiveFromYDB(x *Ydb.Value) (v V, primitive bool) {
	switch v := x.Value.(type) {
	case *Ydb.Value_BoolValue:
		return BoolValue(v.BoolValue), true
	case *Ydb.Value_Int32Value:
		return Int32Value(v.Int32Value), true
	case *Ydb.Value_Uint32Value:
		return Uint32Value(v.Uint32Value), true
	case *Ydb.Value_Int64Value:
		return Int64Value(v.Int64Value), true
	case *Ydb.Value_Uint64Value:
		return Uint64Value(v.Uint64Value), true
	case *Ydb.Value_FloatValue:
		return FloatValue(v.FloatValue), true
	case *Ydb.Value_DoubleValue:
		return DoubleValue(v.DoubleValue), true
	case *Ydb.Value_BytesValue:
		return StringValue(v.BytesValue), true
	case *Ydb.Value_TextValue:
		return UTF8Value(v.TextValue), true
	case *Ydb.Value_Low_128:
		return UUIDValue(BigEndianUint128(x.High_128, v.Low_128)), true
	case *Ydb.Value_NullFlagValue:
		return nil, true
	default:
		return nil, false
	}
}

// BigEndianUint128 builds a big-endian uint128 value.
func BigEndianUint128(hi, lo uint64) (v [16]byte) {
	binary.BigEndian.PutUint64(v[0:8], hi)
	binary.BigEndian.PutUint64(v[8:16], lo)
	return v
}

func FromYDB(t *Ydb.Type, v *Ydb.Value) V {
	tt := TypeFromYDB(t)
	switch t := tt.(type) {
	case PrimitiveType:
		switch t {
		case TypeBool:
			return BoolValue(v.GetBoolValue())

		case TypeInt8:
			return Int8Value(int8(v.GetInt32Value()))

		case TypeInt16:
			return Int16Value(int16(v.GetInt32Value()))

		case TypeInt32:
			return Int32Value(v.GetInt32Value())

		case TypeInt64:
			return Int64Value(v.GetInt64Value())

		case TypeUint8:
			return Uint8Value(uint8(v.GetUint32Value()))

		case TypeUint16:
			return Uint16Value(uint16(v.GetUint32Value()))

		case TypeUint32:
			return Uint32Value(v.GetUint32Value())

		case TypeUint64:
			return Uint64Value(v.GetUint64Value())

		case TypeDate:
			return DateValue(v.GetUint32Value())

		case TypeDatetime:
			return DatetimeValue(v.GetUint32Value())

		case TypeInterval:
			return IntervalValue(v.GetInt64Value())

		case TypeTimestamp:
			return TimestampValue(v.GetUint64Value())

		case TypeFloat:
			return FloatValue(v.GetFloatValue())

		case TypeDouble:
			return DoubleValue(v.GetDoubleValue())

		case TypeUTF8:
			return UTF8Value(v.GetTextValue())

		case TypeYSON:
			return YSONValue(v.GetTextValue())

		case TypeJSON:
			return JSONValue(v.GetTextValue())

		case TypeJSONDocument:
			return JSONDocumentValue(v.GetTextValue())

		case TypeDyNumber:
			return DyNumberValue(v.GetTextValue())

		case TypeTzDate:
			return TzDateValue(v.GetTextValue())

		case TypeTzDatetime:
			return TzDatetimeValue(v.GetTextValue())

		case TypeTzTimestamp:
			return TzTimestampValue(v.GetTextValue())

		case TypeString:
			return StringValue(v.GetBytesValue())

		case TypeUUID:
			return UUIDValue(BigEndianUint128(v.High_128, v.GetLow_128()))

		default:
			panic("uncovered primitive types")
		}

	case *voidType:
		return VoidValue()

	// TODO: check other types
	//case *optionalType:
	//	return OptionalValue(v.Value.)
	//	vv.Value = a.NullFlag()
	//
	//case *listType, *TupleType, *StructType, *dictType:
	//	// Nothing to do.
	//
	//case *DecimalType:
	//	vv.Value = a.Low128()
	//
	//case *variantType:
	//	panic("do not know what to do with variant types for zero value")
	//
	default:
		panic("uncovered types")
	}

	return nil
}
