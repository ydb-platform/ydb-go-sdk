package value

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type V interface {
	Type() T
	String() string

	toYDB(a *allocator.Allocator) *Ydb.Value
	toString(*bytes.Buffer)
}

func ToYDB(v V, a *allocator.Allocator) *Ydb.TypedValue {
	tv := a.TypedValue()

	tv.Type = v.Type().toYDB(a)
	tv.Value = v.toYDB(a)

	return tv
}

func valueToString(buf *bytes.Buffer, t T, v *Ydb.Value) {
	buf.WriteByte('(')
	defer buf.WriteByte(')')
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
	if x, ok := primitiveGoTypeFromYDB(v); ok {
		if x != nil {
			fmt.Fprintf(buf, "%v", x)
		} else {
			buf.WriteString("NULL")
		}
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

func primitiveGoTypeFromYDB(x *Ydb.Value) (v interface{}, primitive bool) {
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
	case *Ydb.Value_NestedValue:
		return primitiveGoTypeFromYDB(v.NestedValue)
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

func nullValueFromYDB(x *Ydb.Value, t T) (_ V, ok bool) {
	for {
		switch xx := x.Value.(type) {
		case *Ydb.Value_NestedValue:
			x = xx.NestedValue
		case *Ydb.Value_NullFlagValue:
			switch tt := t.(type) {
			case *optionalType:
				return NullValue(tt.t), true
			case voidType:
				return VoidValue(), true
			default:
				return nil, false
			}
		default:
			return nil, false
		}
	}
}

func primitiveValueFromYDB(t PrimitiveType, v *Ydb.Value) (V, error) {
	switch t {
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
		return nil, xerrors.WithStackTrace(fmt.Errorf("uncovered primitive type: %T", t))
	}
}

func fromYDB(t *Ydb.Type, v *Ydb.Value) (V, error) {
	tt := TypeFromYDB(t)

	if vv, ok := nullValueFromYDB(v, tt); ok {
		return vv, nil
	}

	switch ttt := tt.(type) {
	case PrimitiveType:
		return primitiveValueFromYDB(ttt, v)

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

	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("uncovered type: %T", ttt))
	}
}

type boolValue bool

func (v boolValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v boolValue) String() string {
	if v {
		return "Bool(true)"
	}
	return "Bool(false)"
}

func (boolValue) Type() T {
	return TypeBool
}

func (v boolValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bool()

	vv.BoolValue = bool(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func BoolValue(v bool) boolValue {
	return boolValue(v)
}

type dateValue uint32

func (v dateValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v dateValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (dateValue) Type() T {
	return TypeDate
}

func (v dateValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()

	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DateValue(v uint32) dateValue {
	return dateValue(v)
}

type datetimeValue uint32

func (v datetimeValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v datetimeValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (datetimeValue) Type() T {
	return TypeDatetime
}

func (v datetimeValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DatetimeValue(v uint32) datetimeValue {
	return datetimeValue(v)
}

type decimalValue struct {
	v [16]byte
	t *DecimalType
}

func (v decimalValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v decimalValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v decimalValue) Type() T {
	return v.t
}

func (v *decimalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var bytes [16]byte
	if v != nil {
		bytes = v.v
	}
	vv := a.Low128()
	vv.Low_128 = binary.BigEndian.Uint64(bytes[8:16])

	vvv := a.Value()
	vvv.High_128 = binary.BigEndian.Uint64(bytes[0:8])
	vvv.Value = vv

	return vvv
}

func DecimalValue(v [16]byte, precision uint32, scale uint32) *decimalValue {
	return &decimalValue{
		v: v,
		t: &DecimalType{
			Precision: precision,
			Scale:     scale,
		},
	}
}

type (
	DictValueField struct {
		K V
		V V
	}
	dictValue struct {
		t      T
		values []DictValueField
	}
)

func (v *dictValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *dictValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *dictValue) Type() T {
	return v.t
}

func (v *dictValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var values []DictValueField
	if v != nil {
		values = v.values
	}
	vvv := a.Value()

	for _, vv := range values {
		pair := a.Pair()

		pair.Key = vv.K.toYDB(a)
		pair.Payload = vv.V.toYDB(a)

		vvv.Pairs = append(vvv.Pairs, pair)
	}

	return vvv
}

func DictValue(values ...DictValueField) *dictValue {
	return &dictValue{
		t:      Dict(values[0].K.Type(), values[0].V.Type()),
		values: values,
	}
}

type doubleValue struct {
	v float64
}

func (v *doubleValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *doubleValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*doubleValue) Type() T {
	return TypeDouble
}

func (v *doubleValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Double()
	if v != nil {
		vv.DoubleValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DoubleValue(v float64) *doubleValue {
	return &doubleValue{v: v}
}

type dyNumberValue struct {
	v string
}

func (v dyNumberValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v dyNumberValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (dyNumberValue) Type() T {
	return TypeDyNumber
}

func (v *dyNumberValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DyNumberValue(v string) *dyNumberValue {
	return &dyNumberValue{v: v}
}

type floatValue struct {
	v float32
}

func (v *floatValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *floatValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*floatValue) Type() T {
	return TypeFloat
}

func (v *floatValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Float()
	if v != nil {
		vv.FloatValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func FloatValue(v float32) *floatValue {
	return &floatValue{v: v}
}

type int8Value int8

func (v int8Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v int8Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (int8Value) Type() T {
	return TypeInt8
}

func (v int8Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int32()
	vv.Int32Value = int32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int8Value(v int8) int8Value {
	return int8Value(v)
}

type int16Value int16

func (v int16Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v int16Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (int16Value) Type() T {
	return TypeInt16
}

func (v int16Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int32()
	vv.Int32Value = int32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int16Value(v int16) int16Value {
	return int16Value(v)
}

type int32Value int32

func (v int32Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v int32Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (int32Value) Type() T {
	return TypeInt32
}

func (v int32Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int32()
	vv.Int32Value = int32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int32Value(v int32) int32Value {
	return int32Value(v)
}

type int64Value int64

func (v int64Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v int64Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (int64Value) Type() T {
	return TypeInt64
}

func (v int64Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int64()
	vv.Int64Value = int64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Int64Value(v int64) int64Value {
	return int64Value(v)
}

type intervalValue int64

func (v intervalValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v intervalValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (intervalValue) Type() T {
	return TypeInterval
}

func (v intervalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int64()
	vv.Int64Value = int64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

// IntervalValue makes Value from given microseconds value
func IntervalValue(v int64) intervalValue {
	return intervalValue(v)
}

type jsonValue struct {
	v string
}

func (v *jsonValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *jsonValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*jsonValue) Type() T {
	return TypeJSON
}

func (v *jsonValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONValue(v string) *jsonValue {
	return &jsonValue{v: v}
}

type jsonDocumentValue struct {
	v string
}

func (v *jsonDocumentValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *jsonDocumentValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*jsonDocumentValue) Type() T {
	return TypeJSONDocument
}

func (v *jsonDocumentValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONDocumentValue(v string) *jsonDocumentValue {
	return &jsonDocumentValue{v: v}
}

type listValue struct {
	t     T
	items []V
}

func (v *listValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *listValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *listValue) Type() T {
	return v.t
}

func (v *listValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var items []V
	if v != nil {
		items = v.items
	}
	vvv := a.Value()

	for _, vv := range items {
		vvv.Items = append(vvv.Items, vv.toYDB(a))
	}

	return vvv
}

func ListValue(items ...V) *listValue {
	var t T
	switch {
	case len(items) > 0:
		t = List(items[0].Type())
	default:
		t = EmptyList()
	}

	for _, v := range items {
		if !v.Type().equalsTo(v.Type()) {
			panic(fmt.Sprintf("different types of items: %v", items))
		}
	}
	return &listValue{
		t:     t,
		items: items,
	}
}

type nullValue struct {
	t *optionalType
}

func (v *nullValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *nullValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *nullValue) Type() T {
	return v.t
}

func (v *nullValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Value()
	vv.Value = a.NullFlag()

	x := v.t.t
	for {
		opt, ok := x.(*optionalType)
		if !ok {
			break
		}
		x = opt.t
		nestedValue := a.Nested()
		nestedValue.NestedValue = vv
		vv = a.Value()
		vv.Value = nestedValue
	}

	return vv
}

func NullValue(t T) *nullValue {
	return &nullValue{
		t: Optional(t),
	}
}

type optionalValue struct {
	t T
	v V
}

func (v *optionalValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *optionalValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *optionalValue) Type() T {
	return v.t
}

func (v *optionalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	if _, opt := v.v.(*optionalValue); opt {
		vv := a.Nested()
		vv.NestedValue = v.v.toYDB(a)
		vvv.Value = vv
	} else {
		vvv.Value = v.v.toYDB(a).Value
	}

	return vvv
}

func OptionalValue(v V) *optionalValue {
	return &optionalValue{
		t: Optional(v.Type()),
		v: v,
	}
}

type (
	StructValueField struct {
		Name string
		V    V
	}
	structValue struct {
		t      T
		values []V
	}
)

func (v *structValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *structValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *structValue) Type() T {
	return v.t
}

func (v structValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for _, vv := range v.values {
		vvv.Items = append(vvv.Items, vv.toYDB(a))
	}

	return vvv
}

func StructValue(fields ...StructValueField) *structValue {
	var (
		structFields = make([]StructField, 0, len(fields))
		values       = make([]V, 0, len(fields))
	)
	for _, field := range fields {
		structFields = append(structFields, StructField{field.Name, field.V.Type()})
		values = append(values, field.V)
	}
	return &structValue{
		t:      Struct(structFields...),
		values: values,
	}
}

type timestampValue uint64

func (v timestampValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v timestampValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (timestampValue) Type() T {
	return TypeTimestamp
}

func (v timestampValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint64()
	vv.Uint64Value = uint64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TimestampValue(v uint64) timestampValue {
	return timestampValue(v)
}

type tupleValue struct {
	t     T
	items []V
}

func (v *tupleValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *tupleValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *tupleValue) Type() T {
	return v.t
}

func (v *tupleValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var items []V
	if v != nil {
		items = v.items
	}
	vvv := a.Value()

	for _, vv := range items {
		vvv.Items = append(vvv.Items, vv.toYDB(a))
	}

	return vvv
}

func TupleValue(values ...V) *tupleValue {
	tupleItems := make([]T, 0, len(values))
	for _, v := range values {
		tupleItems = append(tupleItems, v.Type())
	}
	return &tupleValue{
		t:     Tuple(tupleItems...),
		items: values,
	}
}

type tzDateValue struct {
	v string
}

func (v *tzDateValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *tzDateValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*tzDateValue) Type() T {
	return TypeTzDate
}

func (v *tzDateValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzDateValue(v string) *tzDateValue {
	return &tzDateValue{v: v}
}

type tzDatetimeValue struct {
	v string
}

func (v *tzDatetimeValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *tzDatetimeValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*tzDatetimeValue) Type() T {
	return TypeTzDatetime
}

func (v *tzDatetimeValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzDatetimeValue(v string) *tzDatetimeValue {
	return &tzDatetimeValue{v: v}
}

type tzTimestampValue struct {
	v string
}

func (v *tzTimestampValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *tzTimestampValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*tzTimestampValue) Type() T {
	return TypeTzTimestamp
}

func (v *tzTimestampValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzTimestampValue(v string) *tzTimestampValue {
	return &tzTimestampValue{v: v}
}

type uint8Value uint8

func (v uint8Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v uint8Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (uint8Value) Type() T {
	return TypeUint8
}

func (v uint8Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint8Value(v uint8) uint8Value {
	return uint8Value(v)
}

type uint16Value uint16

func (v uint16Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v uint16Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (uint16Value) Type() T {
	return TypeUint16
}

func (v uint16Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint16Value(v uint16) uint16Value {
	return uint16Value(v)
}

type uint32Value uint32

func (v uint32Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v uint32Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (uint32Value) Type() T {
	return TypeUint32
}

func (v uint32Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint32Value(v uint32) uint32Value {
	return uint32Value(v)
}

type uint64Value uint64

func (v uint64Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v uint64Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (uint64Value) Type() T {
	return TypeUint64
}

func (v uint64Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint64()
	vv.Uint64Value = uint64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func Uint64Value(v uint64) uint64Value {
	return uint64Value(v)
}

type utf8Value struct {
	v string
}

func (v *utf8Value) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *utf8Value) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*utf8Value) Type() T {
	return TypeUTF8
}

func (v *utf8Value) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func UTF8Value(v string) *utf8Value {
	return &utf8Value{v: v}
}

type uuidValue struct {
	v [16]byte
}

func (v *uuidValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *uuidValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*uuidValue) Type() T {
	return TypeUUID
}

func (v *uuidValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var bytes [16]byte
	if v != nil {
		bytes = v.v
	}
	vv := a.Low128()
	vv.Low_128 = binary.BigEndian.Uint64(bytes[8:16])

	vvv := a.Value()
	vvv.High_128 = binary.BigEndian.Uint64(bytes[0:8])
	vvv.Value = vv

	return vvv
}

func UUIDValue(v [16]byte) *uuidValue {
	return &uuidValue{v: v}
}

type variantValue struct {
	t   T
	v   V
	idx uint32
}

func (v *variantValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *variantValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *variantValue) Type() T {
	return v.t
}

func (v *variantValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	nested := a.Nested()
	nested.NestedValue = v.v.toYDB(a)

	vvv.Value = nested
	vvv.VariantIndex = v.idx

	return vvv
}

func VariantValue(v V, idx uint32, t T) *variantValue {
	return &variantValue{
		t:   Variant(t),
		v:   v,
		idx: idx,
	}
}

func VariantValueStruct(v V, idx uint32) *variantValue {
	if _, ok := v.(*structValue); !ok {
		panic("value must be a struct type")
	}
	return &variantValue{
		t: &variantType{
			t:  v.Type(),
			tt: variantTypeStruct,
		},
		v:   v,
		idx: idx,
	}
}

func VariantValueTuple(v V, idx uint32) *variantValue {
	if _, ok := v.(*tupleValue); !ok {
		panic("value must be a tuple type")
	}
	return &variantValue{
		t: &variantType{
			t:  v.Type(),
			tt: variantTypeTuple,
		},
		v:   v,
		idx: idx,
	}
}

type voidValue struct{}

func (v voidValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v voidValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

var (
	_voidValueType = voidType{}
	_voidValue     = &Ydb.Value{
		Value: new(Ydb.Value_NullFlagValue),
	}
)

func (voidValue) Type() T {
	return _voidValueType
}

func (voidValue) toYDB(*allocator.Allocator) *Ydb.Value {
	return _voidValue
}

func VoidValue() voidValue {
	return voidValue{}
}

type ysonValue struct {
	v string
}

func (v *ysonValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *ysonValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*ysonValue) Type() T {
	return TypeYSON
}

func (v *ysonValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func YSONValue(v string) *ysonValue {
	return &ysonValue{v: v}
}

type zeroValue struct {
	t T
}

func (v *zeroValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *zeroValue) String() string {
	buf := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *zeroValue) Type() T {
	return v.t
}

func (v *zeroValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Value()
	switch t := v.t.(type) {
	case PrimitiveType:
		switch t {
		case TypeBool:
			vv.Value = a.Bool()

		case TypeInt8, TypeInt16, TypeInt32:
			vv.Value = a.Int32()

		case
			TypeUint8, TypeUint16, TypeUint32,
			TypeDate, TypeDatetime:

			vv.Value = a.Uint32()

		case
			TypeInt64,
			TypeInterval:

			vv.Value = a.Int64()

		case
			TypeUint64,
			TypeTimestamp:

			vv.Value = a.Uint64()

		case TypeFloat:
			vv.Value = a.Float()

		case TypeDouble:
			vv.Value = a.Double()

		case
			TypeUTF8, TypeYSON, TypeJSON, TypeJSONDocument, TypeDyNumber,
			TypeTzDate, TypeTzDatetime, TypeTzTimestamp:

			vv.Value = a.Text()

		case TypeString:
			vv.Value = a.Bytes()

		case TypeUUID:
			vv.Value = a.Low128()

		default:
			panic("uncovered primitive types")
		}

	case *optionalType, *voidType:
		vv.Value = a.NullFlag()

	case *listType, *TupleType, *StructType, *dictType:
		// Nothing to do.

	case *DecimalType:
		vv.Value = a.Low128()

	case *variantType:
		panic("do not know what to do with variant types for zero value")

	default:
		panic("uncovered types")
	}

	return vv
}

func ZeroValue(t T) *zeroValue {
	return &zeroValue{
		t: t,
	}
}
