package value

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/decimal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type Value interface {
	Type() Type
	Yql() string

	castTo(dst interface{}) error
	toYDB(a *allocator.Allocator) *Ydb.Value
}

func ToYDB(v Value, a *allocator.Allocator) *Ydb.TypedValue {
	tv := a.TypedValue()

	tv.Type = v.Type().toYDB(a)
	tv.Value = v.toYDB(a)

	return tv
}

// BigEndianUint128 builds a big-endian uint128 value.
func BigEndianUint128(hi, lo uint64) (v [16]byte) {
	binary.BigEndian.PutUint64(v[0:8], hi)
	binary.BigEndian.PutUint64(v[8:16], lo)
	return v
}

func FromYDB(t *Ydb.Type, v *Ydb.Value) Value {
	vv, err := fromYDB(t, v)
	if err != nil {
		panic(err)
	}
	return vv
}

func nullValueFromYDB(x *Ydb.Value, t Type) (_ Value, ok bool) {
	for {
		switch xx := x.Value.(type) {
		case *Ydb.Value_NestedValue:
			x = xx.NestedValue
		case *Ydb.Value_NullFlagValue:
			switch tt := t.(type) {
			case optionalType:
				return NullValue(tt.innerType), true
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

func primitiveValueFromYDB(t PrimitiveType, v *Ydb.Value) (Value, error) {
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

	case TypeText:
		return TextValue(v.GetTextValue()), nil

	case TypeYSON:
		switch vv := v.GetValue().(type) {
		case *Ydb.Value_TextValue:
			return YSONValue(xstring.ToBytes(vv.TextValue)), nil
		case *Ydb.Value_BytesValue:
			return YSONValue(vv.BytesValue), nil
		default:
			return nil, xerrors.WithStackTrace(fmt.Errorf("uncovered YSON internal type: %T", vv))
		}

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

	case TypeBytes:
		return BytesValue(v.GetBytesValue()), nil

	case TypeUUID:
		return UUIDValue(BigEndianUint128(v.High_128, v.GetLow_128())), nil

	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("uncovered primitive type: %T", t))
	}
}

func fromYDB(t *Ydb.Type, v *Ydb.Value) (Value, error) {
	tt := TypeFromYDB(t)

	if vv, ok := nullValueFromYDB(v, tt); ok {
		return vv, nil
	}

	switch ttt := tt.(type) {
	case PrimitiveType:
		return primitiveValueFromYDB(ttt, v)

	case voidType:
		return VoidValue(), nil

	case nullType:
		return NullValue(tt), nil

	case *DecimalType:
		return DecimalValue(BigEndianUint128(v.High_128, v.GetLow_128()), ttt.Precision, ttt.Scale), nil

	case optionalType:
		t = t.Type.(*Ydb.Type_OptionalType).OptionalType.Item
		if nestedValue, ok := v.Value.(*Ydb.Value_NestedValue); ok {
			return OptionalValue(FromYDB(t, nestedValue.NestedValue)), nil
		}
		return OptionalValue(FromYDB(t, v)), nil

	case *listType:
		return ListValue(func() []Value {
			vv := make([]Value, len(v.GetItems()))
			a := allocator.New()
			defer a.Free()
			for i, vvv := range v.GetItems() {
				vv[i] = FromYDB(ttt.itemType.toYDB(a), vvv)
			}
			return vv
		}()...), nil

	case *TupleType:
		return TupleValue(func() []Value {
			vv := make([]Value, len(v.GetItems()))
			a := allocator.New()
			defer a.Free()
			for i, vvv := range v.GetItems() {
				vv[i] = FromYDB(ttt.items[i].toYDB(a), vvv)
			}
			return vv
		}()...), nil

	case *StructType:
		return StructValue(func() []StructValueField {
			vv := make([]StructValueField, len(v.GetItems()))
			a := allocator.New()
			defer a.Free()
			for i, vvv := range v.GetItems() {
				vv[i] = StructValueField{
					Name: ttt.fields[i].Name,
					V:    FromYDB(ttt.fields[i].T.toYDB(a), vvv),
				}
			}
			return vv
		}()...), nil

	case *dictType:
		return DictValue(func() []DictValueField {
			vv := make([]DictValueField, len(v.GetPairs()))
			a := allocator.New()
			defer a.Free()
			for i, vvv := range v.GetPairs() {
				vv[i] = DictValueField{
					K: FromYDB(ttt.keyType.toYDB(a), vvv.Key),
					V: FromYDB(ttt.valueType.toYDB(a), vvv.Payload),
				}
			}
			return vv
		}()...), nil

	case *setType:
		return SetValue(func() []Value {
			vv := make([]Value, len(v.GetPairs()))
			a := allocator.New()
			defer a.Free()
			for i, vvv := range v.GetPairs() {
				vv[i] = FromYDB(ttt.itemType.toYDB(a), vvv.Key)
			}
			return vv
		}()...), nil

	case *variantStructType:
		a := allocator.New()
		defer a.Free()
		return VariantValueStruct(
			FromYDB(
				ttt.StructType.fields[v.VariantIndex].T.toYDB(a),
				v.Value.(*Ydb.Value_NestedValue).NestedValue,
			),
			ttt.StructType.fields[v.VariantIndex].Name,
			ttt.StructType,
		), nil

	case *variantTupleType:
		a := allocator.New()
		defer a.Free()
		return VariantValueTuple(
			FromYDB(
				ttt.TupleType.items[v.VariantIndex].toYDB(a),
				v.Value.(*Ydb.Value_NestedValue).NestedValue,
			),
			v.VariantIndex,
			ttt.TupleType,
		), nil

	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("uncovered type: %T", ttt))
	}
}

type boolValue bool

func (v boolValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *bool:
		*vv = bool(v)
		return nil
	case *string:
		*vv = strconv.FormatBool(bool(v))
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v boolValue) Yql() string {
	return strconv.FormatBool(bool(v))
}

func (boolValue) Type() Type {
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

func (v dateValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *time.Time:
		*vv = DateToTime(uint32(v))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *int32:
		*vv = int32(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v dateValue) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), DateToTime(uint32(v)).UTC().Format(LayoutDate))
}

func (dateValue) Type() Type {
	return TypeDate
}

func (v dateValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()

	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

// DateValue returns ydb date value by given days since Epoch
func DateValue(v uint32) dateValue {
	return dateValue(v)
}

func DateValueFromTime(t time.Time) dateValue {
	return dateValue(uint64(t.Sub(epoch)/time.Second) / secondsPerDay)
}

type datetimeValue uint32

func (v datetimeValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *time.Time:
		*vv = DatetimeToTime(uint32(v))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *uint32:
		*vv = uint32(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v datetimeValue) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), DatetimeToTime(uint32(v)).UTC().Format(LayoutDatetime))
}

func (datetimeValue) Type() Type {
	return TypeDatetime
}

func (v datetimeValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

// DatetimeValue makes ydb datetime value from seconds since Epoch
func DatetimeValue(v uint32) datetimeValue {
	return datetimeValue(v)
}

func DatetimeValueFromTime(t time.Time) datetimeValue {
	return datetimeValue(t.Unix())
}

var _ DecimalValuer = (*decimalValue)(nil)

type decimalValue struct {
	value     [16]byte
	innerType *DecimalType
}

func (v *decimalValue) Value() [16]byte {
	return v.value
}

func (v *decimalValue) Precision() uint32 {
	return v.innerType.Precision
}

func (v *decimalValue) Scale() uint32 {
	return v.innerType.Scale
}

type DecimalValuer interface {
	Value() [16]byte
	Precision() uint32
	Scale() uint32
}

func (v *decimalValue) castTo(dst interface{}) error {
	return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' to '%T' destination", v, dst))
}

func (v *decimalValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString(v.innerType.Name())
	buffer.WriteByte('(')
	buffer.WriteByte('"')
	s := decimal.FromBytes(v.value[:], v.innerType.Precision, v.innerType.Scale).String()
	buffer.WriteString(s[:len(s)-int(v.innerType.Scale)] + "." + s[len(s)-int(v.innerType.Scale):])
	buffer.WriteByte('"')
	buffer.WriteByte(',')
	buffer.WriteString(strconv.FormatUint(uint64(v.innerType.Precision), 10))
	buffer.WriteByte(',')
	buffer.WriteString(strconv.FormatUint(uint64(v.innerType.Scale), 10))
	buffer.WriteByte(')')
	return buffer.String()
}

func (v *decimalValue) Type() Type {
	return v.innerType
}

func (v *decimalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var bytes [16]byte
	if v != nil {
		bytes = v.value
	}
	vv := a.Low128()
	vv.Low_128 = binary.BigEndian.Uint64(bytes[8:16])

	vvv := a.Value()
	vvv.High_128 = binary.BigEndian.Uint64(bytes[0:8])
	vvv.Value = vv

	return vvv
}

func DecimalValueFromBigInt(v *big.Int, precision, scale uint32) *decimalValue {
	b := decimal.BigIntToByte(v, precision, scale)
	return DecimalValue(b, precision, scale)
}

func DecimalValue(v [16]byte, precision, scale uint32) *decimalValue {
	return &decimalValue{
		value: v,
		innerType: &DecimalType{
			Precision: precision,
			Scale:     scale,
		},
	}
}

type (
	DictValueField struct {
		K Value
		V Value
	}
	dictValue struct {
		t      Type
		values []DictValueField
	}
)

func (v *dictValue) DictValues() map[Value]Value {
	values := make(map[Value]Value, len(v.values))
	for i := range v.values {
		values[v.values[i].K] = v.values[i].V
	}
	return values
}

func (v *dictValue) castTo(dst interface{}) error {
	return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' to '%T' destination", v, dst))
}

func (v *dictValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteByte('{')
	for i := range v.values {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(v.values[i].K.Yql())
		buffer.WriteByte(':')
		buffer.WriteString(v.values[i].V.Yql())
	}
	buffer.WriteByte('}')
	return buffer.String()
}

func (v *dictValue) Type() Type {
	return v.t
}

func (v *dictValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var values []DictValueField
	if v != nil {
		values = v.values
	}
	vvv := a.Value()

	for i := range values {
		pair := a.Pair()

		pair.Key = values[i].K.toYDB(a)
		pair.Payload = values[i].V.toYDB(a)

		vvv.Pairs = append(vvv.Pairs, pair)
	}

	return vvv
}

func DictValue(values ...DictValueField) *dictValue {
	sort.Slice(values, func(i, j int) bool {
		return values[i].K.Yql() < values[j].K.Yql()
	})
	var t Type
	switch {
	case len(values) > 0:
		t = Dict(values[0].K.Type(), values[0].V.Type())
	default:
		t = EmptyDict()
	}
	return &dictValue{
		t:      t,
		values: values,
	}
}

type doubleValue struct {
	value float64
}

func (v *doubleValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatFloat(v.value, 'f', -1, 64)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatFloat(v.value, 'f', -1, 64))
		return nil
	case *float64:
		*vv = v.value
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v *doubleValue) Yql() string {
	return fmt.Sprintf("%s(\"%v\")", v.Type().Yql(), v.value)
}

func (*doubleValue) Type() Type {
	return TypeDouble
}

func (v *doubleValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Double()
	if v != nil {
		vv.DoubleValue = v.value
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DoubleValue(v float64) *doubleValue {
	return &doubleValue{value: v}
}

type dyNumberValue string

func (v dyNumberValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(string(v))
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v dyNumberValue) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), string(v))
}

func (dyNumberValue) Type() Type {
	return TypeDyNumber
}

func (v dyNumberValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	vv.TextValue = string(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DyNumberValue(v string) dyNumberValue {
	return dyNumberValue(v)
}

type floatValue struct {
	value float32
}

func (v *floatValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatFloat(float64(v.value), 'f', -1, 32)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatFloat(float64(v.value), 'f', -1, 32))
		return nil
	case *float64:
		*vv = float64(v.value)
		return nil
	case *float32:
		*vv = v.value
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v *floatValue) Yql() string {
	return fmt.Sprintf("%s(\"%v\")", v.Type().Yql(), v.value)
}

func (*floatValue) Type() Type {
	return TypeFloat
}

func (v *floatValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Float()
	if v != nil {
		vv.FloatValue = v.value
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func FloatValue(v float32) *floatValue {
	return &floatValue{value: v}
}

type int8Value int8

func (v int8Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatInt(int64(v), 10))
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *int32:
		*vv = int32(v)
		return nil
	case *int16:
		*vv = int16(v)
		return nil
	case *int8:
		*vv = int8(v)
		return nil
	case *float64:
		*vv = float64(v)
		return nil
	case *float32:
		*vv = float32(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v int8Value) Yql() string {
	return strconv.FormatUint(uint64(v), 10) + "t"
}

func (int8Value) Type() Type {
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

func (v int16Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatInt(int64(v), 10))
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *int32:
		*vv = int32(v)
		return nil
	case *int16:
		*vv = int16(v)
		return nil
	case *float64:
		*vv = float64(v)
		return nil
	case *float32:
		*vv = float32(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v int16Value) Yql() string {
	return strconv.FormatUint(uint64(v), 10) + "s"
}

func (int16Value) Type() Type {
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

func (v int32Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatInt(int64(v), 10))
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *int:
		*vv = int(v)
		return nil
	case *int32:
		*vv = int32(v)
		return nil
	case *float64:
		*vv = float64(v)
		return nil
	case *float32:
		*vv = float32(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v int32Value) Yql() string {
	return strconv.FormatInt(int64(v), 10)
}

func (int32Value) Type() Type {
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

func (v int64Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatInt(int64(v), 10))
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *float64:
		*vv = float64(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v int64Value) Yql() string {
	return strconv.FormatUint(uint64(v), 10) + "l"
}

func (int64Value) Type() Type {
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

func (v intervalValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *time.Duration:
		*vv = IntervalToDuration(int64(v))
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v intervalValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString(v.Type().Yql())
	buffer.WriteByte('(')
	buffer.WriteByte('"')
	d := IntervalToDuration(int64(v))
	if d < 0 {
		buffer.WriteByte('-')
		d = -d
	}
	buffer.WriteByte('P')
	if days := d / time.Hour / 24; days > 0 {
		d -= days * time.Hour * 24 //nolint:durationcheck
		buffer.WriteString(strconv.FormatInt(int64(days), 10))
		buffer.WriteByte('D')
	}
	if d > 0 {
		buffer.WriteByte('T')
	}
	if hours := d / time.Hour; hours > 0 {
		d -= hours * time.Hour //nolint:durationcheck
		buffer.WriteString(strconv.FormatInt(int64(hours), 10))
		buffer.WriteByte('H')
	}
	if minutes := d / time.Minute; minutes > 0 {
		d -= minutes * time.Minute //nolint:durationcheck
		buffer.WriteString(strconv.FormatInt(int64(minutes), 10))
		buffer.WriteByte('M')
	}
	if d > 0 {
		seconds := float64(d) / float64(time.Second)
		fmt.Fprintf(buffer, "%0.6f", seconds)
		buffer.WriteByte('S')
	}
	buffer.WriteByte('"')
	buffer.WriteByte(')')
	return buffer.String()
}

func (intervalValue) Type() Type {
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

func IntervalValueFromDuration(v time.Duration) intervalValue {
	return intervalValue(durationToMicroseconds(v))
}

type jsonValue string

func (v jsonValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(string(v))
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v jsonValue) Yql() string {
	return fmt.Sprintf("%s(@@%s@@)", v.Type().Yql(), string(v))
}

func (jsonValue) Type() Type {
	return TypeJSON
}

func (v jsonValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	vv.TextValue = string(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONValue(v string) jsonValue {
	return jsonValue(v)
}

type jsonDocumentValue string

func (v jsonDocumentValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(string(v))
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v jsonDocumentValue) Yql() string {
	return fmt.Sprintf("%s(@@%s@@)", v.Type().Yql(), string(v))
}

func (jsonDocumentValue) Type() Type {
	return TypeJSONDocument
}

func (v jsonDocumentValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	vv.TextValue = string(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONDocumentValue(v string) jsonDocumentValue {
	return jsonDocumentValue(v)
}

type listValue struct {
	t     Type
	items []Value
}

func (v *listValue) ListItems() []Value {
	return v.items
}

func (v *listValue) castTo(dst interface{}) error {
	return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), dst))
}

func (v *listValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteByte('[')
	for i, item := range v.items {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(item.Yql())
	}
	buffer.WriteByte(']')
	return buffer.String()
}

func (v *listValue) Type() Type {
	return v.t
}

func (v *listValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var items []Value
	if v != nil {
		items = v.items
	}
	vvv := a.Value()

	for _, vv := range items {
		vvv.Items = append(vvv.Items, vv.toYDB(a))
	}

	return vvv
}

func ListValue(items ...Value) *listValue {
	var t Type
	switch {
	case len(items) > 0:
		t = List(items[0].Type())
	default:
		t = EmptyList()
	}
	return &listValue{
		t:     t,
		items: items,
	}
}

type setValue struct {
	t     Type
	items []Value
}

func (v *setValue) castTo(dst interface{}) error {
	return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' to '%T' destination", v, dst))
}

func (v *setValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteByte('{')
	for i, item := range v.items {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(item.Yql())
	}
	buffer.WriteByte('}')
	return buffer.String()
}

func (v *setValue) Type() Type {
	return v.t
}

func (v *setValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for _, vv := range v.items {
		pair := a.Pair()

		pair.Key = vv.toYDB(a)
		pair.Payload = _voidValue

		vvv.Pairs = append(vvv.Pairs, pair)
	}

	return vvv
}

func SetValue(items ...Value) *setValue {
	sort.Slice(items, func(i, j int) bool {
		return items[i].Yql() < items[j].Yql()
	})

	var t Type
	switch {
	case len(items) > 0:
		t = Set(items[0].Type())
	default:
		t = EmptySet()
	}

	return &setValue{
		t:     t,
		items: items,
	}
}

func NullValue(t Type) *optionalValue {
	return &optionalValue{
		innerType: Optional(t),
		value:     nil,
	}
}

type optionalValue struct {
	innerType Type
	value     Value
}

var errOptionalNilValue = errors.New("optional contains nil value")

func (v *optionalValue) castTo(dst interface{}) error {
	if v.value == nil {
		return xerrors.WithStackTrace(errOptionalNilValue)
	}
	return v.value.castTo(dst)
}

func (v *optionalValue) Yql() string {
	if v.value == nil {
		return fmt.Sprintf("Nothing(%s)", v.Type().Yql())
	}
	return fmt.Sprintf("Just(%s)", v.value.Yql())
}

func (v *optionalValue) Type() Type {
	return v.innerType
}

func (v *optionalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Value()
	if _, opt := v.value.(*optionalValue); opt {
		vvv := a.Nested()
		vvv.NestedValue = v.value.toYDB(a)
		vv.Value = vvv
	} else {
		if v.value != nil {
			vv.Value = v.value.toYDB(a).Value
		} else {
			vv.Value = a.NullFlag()
		}
	}
	return vv
}

func OptionalValue(v Value) *optionalValue {
	return &optionalValue{
		innerType: Optional(v.Type()),
		value:     v,
	}
}

type (
	StructValueField struct {
		Name string
		V    Value
	}
	structValue struct {
		t      Type
		fields []StructValueField
	}
)

func (v *structValue) StructFields() map[string]Value {
	fields := make(map[string]Value, len(v.fields))
	for i := range v.fields {
		fields[v.fields[i].Name] = v.fields[i].V
	}
	return fields
}

func (v *structValue) castTo(dst interface{}) error {
	return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' to '%T' destination", v, dst))
}

func (v *structValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString("<|")
	for i := range v.fields {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString("`" + v.fields[i].Name + "`:")
		buffer.WriteString(v.fields[i].V.Yql())
	}
	buffer.WriteString("|>")
	return buffer.String()
}

func (v *structValue) Type() Type {
	return v.t
}

func (v *structValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for i := range v.fields {
		vvv.Items = append(vvv.Items, v.fields[i].V.toYDB(a))
	}

	return vvv
}

func StructValue(fields ...StructValueField) *structValue {
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	structFields := make([]StructField, 0, len(fields))
	for i := range fields {
		structFields = append(structFields, StructField{fields[i].Name, fields[i].V.Type()})
	}
	return &structValue{
		t:      Struct(structFields...),
		fields: fields,
	}
}

type timestampValue uint64

func (v timestampValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *time.Time:
		*vv = TimestampToTime(uint64(v))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v timestampValue) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), TimestampToTime(uint64(v)).UTC().Format(LayoutTimestamp))
}

func (timestampValue) Type() Type {
	return TypeTimestamp
}

func (v timestampValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint64()
	vv.Uint64Value = uint64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

// TimestampValue makes ydb timestamp value by given microseconds since Epoch
func TimestampValue(v uint64) timestampValue {
	return timestampValue(v)
}

func TimestampValueFromTime(t time.Time) timestampValue {
	return timestampValue(t.Sub(epoch) / time.Microsecond)
}

type tupleValue struct {
	t     Type
	items []Value
}

func (v *tupleValue) TupleItems() []Value {
	return v.items
}

func (v *tupleValue) castTo(dst interface{}) error {
	if len(v.items) == 1 {
		return v.items[0].castTo(dst)
	}
	return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' to '%T' destination", v, dst))
}

func (v *tupleValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteByte('(')
	for i, item := range v.items {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(item.Yql())
	}
	buffer.WriteByte(')')
	return buffer.String()
}

func (v *tupleValue) Type() Type {
	return v.t
}

func (v *tupleValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var items []Value
	if v != nil {
		items = v.items
	}
	vvv := a.Value()

	for _, vv := range items {
		vvv.Items = append(vvv.Items, vv.toYDB(a))
	}

	return vvv
}

func TupleValue(values ...Value) *tupleValue {
	tupleItems := make([]Type, 0, len(values))
	for _, v := range values {
		tupleItems = append(tupleItems, v.Type())
	}
	return &tupleValue{
		t:     Tuple(tupleItems...),
		items: values,
	}
}

type tzDateValue string

func (v tzDateValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(string(v))
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v tzDateValue) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), string(v))
}

func (tzDateValue) Type() Type {
	return TypeTzDate
}

func (v tzDateValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	vv.TextValue = string(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzDateValue(v string) tzDateValue {
	return tzDateValue(v)
}

func TzDateValueFromTime(t time.Time) tzDateValue {
	return tzDateValue(t.Format(LayoutDate))
}

type tzDatetimeValue string

func (v tzDatetimeValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(string(v))
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v tzDatetimeValue) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), string(v))
}

func (tzDatetimeValue) Type() Type {
	return TypeTzDatetime
}

func (v tzDatetimeValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	vv.TextValue = string(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzDatetimeValue(v string) tzDatetimeValue {
	return tzDatetimeValue(v)
}

func TzDatetimeValueFromTime(t time.Time) tzDatetimeValue {
	return tzDatetimeValue(t.Format(LayoutDatetime))
}

type tzTimestampValue string

func (v tzTimestampValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(string(v))
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v tzTimestampValue) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), string(v))
}

func (tzTimestampValue) Type() Type {
	return TypeTzTimestamp
}

func (v tzTimestampValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	vv.TextValue = string(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzTimestampValue(v string) tzTimestampValue {
	return tzTimestampValue(v)
}

func TzTimestampValueFromTime(t time.Time) tzTimestampValue {
	return tzTimestampValue(t.Format(LayoutTimestamp))
}

type uint8Value uint8

func (v uint8Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatInt(int64(v), 10))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *uint32:
		*vv = uint32(v)
		return nil
	case *int32:
		*vv = int32(v)
		return nil
	case *uint16:
		*vv = uint16(v)
		return nil
	case *int16:
		*vv = int16(v)
		return nil
	case *uint8:
		*vv = uint8(v)
		return nil
	case *float64:
		*vv = float64(v)
		return nil
	case *float32:
		*vv = float32(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v uint8Value) Yql() string {
	return strconv.FormatUint(uint64(v), 10) + "ut"
}

func (uint8Value) Type() Type {
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

func (v uint16Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatInt(int64(v), 10))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *uint32:
		*vv = uint32(v)
		return nil
	case *int32:
		*vv = int32(v)
		return nil
	case *uint16:
		*vv = uint16(v)
		return nil
	case *float32:
		*vv = float32(v)
		return nil
	case *float64:
		*vv = float64(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v uint16Value) Yql() string {
	return strconv.FormatUint(uint64(v), 10) + "us"
}

func (uint16Value) Type() Type {
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

func (v uint32Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatInt(int64(v), 10))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *uint32:
		*vv = uint32(v)
		return nil
	case *float64:
		*vv = float64(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v uint32Value) Yql() string {
	return strconv.FormatUint(uint64(v), 10) + "u"
}

func (uint32Value) Type() Type {
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

func (v uint64Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(strconv.FormatInt(int64(v), 10))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v uint64Value) Yql() string {
	return strconv.FormatUint(uint64(v), 10) + "ul"
}

func (uint64Value) Type() Type {
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

type textValue string

func (v textValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = xstring.ToBytes(string(v))
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v textValue) Yql() string {
	return fmt.Sprintf("%qu", string(v))
}

func (textValue) Type() Type {
	return TypeText
}

func (v textValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	vv.TextValue = string(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TextValue(v string) textValue {
	return textValue(v)
}

type uuidValue struct {
	value [16]byte
}

func (v *uuidValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v.value[:])
		return nil
	case *[]byte:
		*vv = v.value[:]
		return nil
	case *[16]byte:
		*vv = v.value
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v *uuidValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString(v.Type().Yql())
	buffer.WriteByte('(')
	buffer.WriteByte('"')
	buffer.WriteString(uuid.UUID(v.value).String())
	buffer.WriteByte('"')
	buffer.WriteByte(')')
	return buffer.String()
}

func (*uuidValue) Type() Type {
	return TypeUUID
}

func (v *uuidValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var bytes [16]byte
	if v != nil {
		bytes = v.value
	}
	vv := a.Low128()
	vv.Low_128 = binary.BigEndian.Uint64(bytes[8:16])

	vvv := a.Value()
	vvv.High_128 = binary.BigEndian.Uint64(bytes[0:8])
	vvv.Value = vv

	return vvv
}

func UUIDValue(v [16]byte) *uuidValue {
	return &uuidValue{value: v}
}

type variantValue struct {
	innerType Type
	value     Value
	idx       uint32
}

func (v *variantValue) Variant() (name string, index uint32) {
	switch t := v.innerType.(type) {
	case *variantStructType:
		return t.fields[v.idx].Name, v.idx
	default:
		return "", v.idx
	}
}

func (v *variantValue) Value() Value {
	return v.value
}

func (v *variantValue) castTo(dst interface{}) error {
	return v.value.castTo(dst)
}

func (v *variantValue) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString("Variant(")
	buffer.WriteString(v.value.Yql())
	buffer.WriteByte(',')
	switch t := v.innerType.(type) {
	case *variantStructType:
		fmt.Fprintf(buffer, "%q", t.fields[v.idx].Name)
	case *variantTupleType:
		fmt.Fprintf(buffer, "\""+strconv.FormatUint(uint64(v.idx), 10)+"\"")
	}
	buffer.WriteByte(',')
	buffer.WriteString(v.Type().Yql())
	buffer.WriteByte(')')
	return buffer.String()
}

func (v *variantValue) Type() Type {
	return v.innerType
}

func (v *variantValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	nested := a.Nested()
	nested.NestedValue = v.value.toYDB(a)

	vvv.Value = nested
	vvv.VariantIndex = v.idx

	return vvv
}

func VariantValueTuple(v Value, idx uint32, t Type) *variantValue {
	if tt, has := t.(*TupleType); has {
		t = VariantTuple(tt.items...)
	}
	return &variantValue{
		innerType: t,
		value:     v,
		idx:       idx,
	}
}

func VariantValueStruct(v Value, name string, t Type) *variantValue {
	var idx int
	switch tt := t.(type) {
	case *StructType:
		sort.Slice(tt.fields, func(i, j int) bool {
			return tt.fields[i].Name < tt.fields[j].Name
		})
		idx = sort.Search(len(tt.fields), func(i int) bool {
			return tt.fields[i].Name >= name
		})
		t = VariantStruct(tt.fields...)
	case *variantStructType:
		sort.Slice(tt.fields, func(i, j int) bool {
			return tt.fields[i].Name < tt.fields[j].Name
		})
		idx = sort.Search(len(tt.fields), func(i int) bool {
			return tt.fields[i].Name >= name
		})
	}
	return &variantValue{
		innerType: t,
		value:     v,
		idx:       uint32(idx),
	}
}

type voidValue struct{}

func (v voidValue) castTo(dst interface{}) error {
	return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%s' to '%T' destination", v.Type().Yql(), dst))
}

func (v voidValue) Yql() string {
	return v.Type().Yql() + "()"
}

var (
	_voidValueType = voidType{}
	_voidValue     = &Ydb.Value{
		Value: new(Ydb.Value_NullFlagValue),
	}
)

func (voidValue) Type() Type {
	return _voidValueType
}

func (voidValue) toYDB(*allocator.Allocator) *Ydb.Value {
	return _voidValue
}

func VoidValue() voidValue {
	return voidValue{}
}

type ysonValue []byte

func (v ysonValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = xstring.FromBytes(v)
		return nil
	case *[]byte:
		*vv = v
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v ysonValue) Yql() string {
	return fmt.Sprintf("%s(%q)", v.Type().Yql(), string(v))
}

func (ysonValue) Type() Type {
	return TypeYSON
}

func (v ysonValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bytes()
	if v != nil {
		vv.BytesValue = v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func YSONValue(v []byte) ysonValue {
	return v
}

func zeroPrimitiveValue(t PrimitiveType) Value {
	switch t {
	case TypeBool:
		return BoolValue(false)

	case TypeInt8:
		return Int8Value(0)

	case TypeUint8:
		return Uint8Value(0)

	case TypeInt16:
		return Int16Value(0)

	case TypeUint16:
		return Uint16Value(0)

	case TypeInt32:
		return Int32Value(0)

	case TypeUint32:
		return Uint32Value(0)

	case TypeInt64:
		return Int64Value(0)

	case TypeUint64:
		return Uint64Value(0)

	case TypeFloat:
		return FloatValue(0)

	case TypeDouble:
		return DoubleValue(0)

	case TypeDate:
		return DateValue(0)

	case TypeDatetime:
		return DatetimeValue(0)

	case TypeTimestamp:
		return TimestampValue(0)

	case TypeInterval:
		return IntervalValue(0)

	case TypeText:
		return TextValue("")

	case TypeYSON:
		return YSONValue([]byte(""))

	case TypeJSON:
		return JSONValue("")

	case TypeJSONDocument:
		return JSONDocumentValue("")

	case TypeDyNumber:
		return DyNumberValue("")

	case TypeTzDate:
		return TzDateValue("")

	case TypeTzDatetime:
		return TzDatetimeValue("")

	case TypeTzTimestamp:
		return TzTimestampValue("")

	case TypeBytes:
		return BytesValue([]byte{})

	case TypeUUID:
		return UUIDValue([16]byte{})

	default:
		panic(fmt.Sprintf("uncovered primitive type '%T'", t))
	}
}

func ZeroValue(t Type) Value {
	switch t := t.(type) {
	case PrimitiveType:
		return zeroPrimitiveValue(t)

	case optionalType:
		return NullValue(t.innerType)

	case *voidType:
		return VoidValue()

	case *listType, *emptyListType:
		return &listValue{
			t: t,
		}
	case *setType:
		return &setValue{
			t: t,
		}
	case *dictType:
		return &dictValue{
			t: t.valueType,
		}
	case *emptyDictType:
		return &dictValue{
			t: t,
		}
	case *TupleType:
		return TupleValue(func() []Value {
			values := make([]Value, len(t.items))
			for i, tt := range t.items {
				values[i] = ZeroValue(tt)
			}
			return values
		}()...)
	case *StructType:
		return StructValue(func() []StructValueField {
			fields := make([]StructValueField, len(t.fields))
			for i := range t.fields {
				fields[i] = StructValueField{
					Name: t.fields[i].Name,
					V:    ZeroValue(t.fields[i].T),
				}
			}
			return fields
		}()...)
	case *DecimalType:
		return DecimalValue([16]byte{}, 22, 9)

	default:
		panic(fmt.Sprintf("type '%T' have not a zero value", t))
	}
}

type bytesValue []byte

func (v bytesValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = xstring.FromBytes(v)
		return nil
	case *[]byte:
		*vv = v
		return nil
	default:
		return xerrors.WithStackTrace(fmt.Errorf("cannot cast '%+v' (type '%s') to '%T' destination", v, v.Type().Yql(), vv))
	}
}

func (v bytesValue) Yql() string {
	return fmt.Sprintf("%q", string(v))
}

func (bytesValue) Type() Type {
	return TypeBytes
}

func (v bytesValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bytes()

	vv.BytesValue = v

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func BytesValue(v []byte) bytesValue {
	return v
}
