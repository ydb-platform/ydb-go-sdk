package value

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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

func primitiveFromYDB(x *Ydb.Value) (v interface{}, primitive bool) {
	switch v := x.Value.(type) {
	case *Ydb.Value_BoolValue:
		return v.BoolValue, true
	case *Ydb.Value_Int32Value:
		return v.Int32Value, true
	case *Ydb.Value_Uint32Value:
		return v.Uint32Value, true
	case *Ydb.Value_Int64Value:
		return v.Int64Value, true
	case *Ydb.Value_Uint64Value:
		return v.Uint64Value, true
	case *Ydb.Value_FloatValue:
		return v.FloatValue, true
	case *Ydb.Value_DoubleValue:
		return v.DoubleValue, true
	case *Ydb.Value_BytesValue:
		return v.BytesValue, true
	case *Ydb.Value_TextValue:
		return v.TextValue, true
	case *Ydb.Value_Low_128:
		return BigEndianUint128(x.High_128, v.Low_128), true
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
	if vv, err := fromYDB(t, v); err != nil {
		panic(err)
	} else {
		return vv
	}
}

func nullValueFromYDB(x *Ydb.Value, t T) (_ *nullValue, ok bool) {
	for {
		switch xx := x.Value.(type) {
		case *Ydb.Value_NestedValue:
			x = xx.NestedValue
		case *Ydb.Value_NullFlagValue:
			return NullValue(t.(*optionalType).t), true
		default:
			return nil, false
		}
	}
}

func fromYDB(t *Ydb.Type, v *Ydb.Value) (V, error) {
	tt := TypeFromYDB(t)

	if vv, ok := nullValueFromYDB(v, tt); ok {
		return vv, nil
	}

	switch ttt := tt.(type) {
	case PrimitiveType:
		switch ttt {
		case TypeBool:
			return BoolValue(v.GetBoolValue()), nil

		case TypeInt8:
			return Int8Value(int8(v.GetInt32Value())), nil

		case TypeInt16:
			return Int16Value(int16(v.GetInt32Value())), nil

		case TypeInt32:
			return Int32Value(v.GetInt32Value()), nil

		case TypeInt64:
			return Int64Value(v.GetInt64Value()), nil

		case TypeUint8:
			return Uint8Value(uint8(v.GetUint32Value())), nil

		case TypeUint16:
			return Uint16Value(uint16(v.GetUint32Value())), nil

		case TypeUint32:
			return Uint32Value(v.GetUint32Value()), nil

		case TypeUint64:
			return Uint64Value(v.GetUint64Value()), nil

		case TypeDate:
			return DateValue(v.GetUint32Value()), nil

		case TypeDatetime:
			return DatetimeValue(v.GetUint32Value()), nil

		case TypeInterval:
			return IntervalValue(v.GetInt64Value()), nil

		case TypeTimestamp:
			return TimestampValue(v.GetUint64Value()), nil

		case TypeFloat:
			return FloatValue(v.GetFloatValue()), nil

		case TypeDouble:
			return DoubleValue(v.GetDoubleValue()), nil

		case TypeUTF8:
			return UTF8Value(v.GetTextValue()), nil

		case TypeYSON:
			return YSONValue(v.GetTextValue()), nil

		case TypeJSON:
			return JSONValue(v.GetTextValue()), nil

		case TypeJSONDocument:
			return JSONDocumentValue(v.GetTextValue()), nil

		case TypeDyNumber:
			return DyNumberValue(v.GetTextValue()), nil

		case TypeTzDate:
			return TzDateValue(v.GetTextValue()), nil

		case TypeTzDatetime:
			return TzDatetimeValue(v.GetTextValue()), nil

		case TypeTzTimestamp:
			return TzTimestampValue(v.GetTextValue()), nil

		case TypeString:
			return StringValue(v.GetBytesValue()), nil

		case TypeUUID:
			return UUIDValue(BigEndianUint128(v.High_128, v.GetLow_128())), nil

		default:
			return nil, xerrors.WithStackTrace(fmt.Errorf("uncovered primitive type: %T", ttt))
		}

	case voidType:
		return VoidValue(), nil

	case *DecimalType:
		return DecimalValue(BigEndianUint128(v.High_128, v.GetLow_128()), ttt.Precision, ttt.Scale), nil

	case *optionalType:
		t = t.Type.(*Ydb.Type_OptionalType).OptionalType.Item
		if nestedValue, ok := v.Value.(*Ydb.Value_NestedValue); ok {
			return OptionalValue(FromYDB(t, nestedValue.NestedValue)), nil
		}
		return OptionalValue(FromYDB(t, v)), nil

	case *listType:
		return ListValue(func() (vv []V) {
			a := allocator.New()
			defer a.Free()
			for _, vvv := range v.Items {
				vv = append(vv, FromYDB(ttt.t.toYDB(a), vvv))
			}
			return vv
		}()...), nil

	case *TupleType:
		return TupleValue(func() (vv []V) {
			a := allocator.New()
			defer a.Free()
			for i, vvv := range v.Items {
				vv = append(vv, FromYDB(ttt.items[i].toYDB(a), vvv))
			}
			return vv
		}()...), nil

	case *StructType:
		return StructValue(func() (vv []StructValueField) {
			a := allocator.New()
			defer a.Free()
			for i, vvv := range v.Items {
				vv = append(vv, StructValueField{
					Name: ttt.fields[i].Name,
					V:    FromYDB(ttt.fields[i].T.toYDB(a), vvv),
				})
			}
			return vv
		}()...), nil

	case *dictType:
		return DictValue(func() (vv []DictValueField) {
			a := allocator.New()
			defer a.Free()
			for _, vvv := range v.Pairs {
				vv = append(vv, DictValueField{
					K: FromYDB(ttt.k.toYDB(a), vvv.Key),
					V: FromYDB(ttt.v.toYDB(a), vvv.Payload),
				})
			}
			return vv
		}()...), nil

	case *variantType:
		a := allocator.New()
		defer a.Free()
		switch ttt.tt {
		case variantTypeTuple:
			return VariantValue(
				FromYDB(ttt.t.(*TupleType).items[v.VariantIndex].toYDB(a), v.Value.(*Ydb.Value_NestedValue).NestedValue),
				v.VariantIndex,
				ttt.t,
			), nil
		case variantTypeStruct:
			return VariantValue(
				FromYDB(ttt.t.(*StructType).fields[v.VariantIndex].T.toYDB(a), v.Value.(*Ydb.Value_NestedValue).NestedValue),
				v.VariantIndex,
				ttt.t,
			), nil
		default:
			return nil, fmt.Errorf("unknown variant type: %v", ttt.tt)
		}

	// TODO: check other types
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
		return nil, xerrors.WithStackTrace(fmt.Errorf("uncovered type: %T", ttt))
	}
}
