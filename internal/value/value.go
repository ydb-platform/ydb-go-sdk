package value

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/decimal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Value interface {
	fmt.Formatter

	Type() Type
	String() string

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

type verbFormatter struct {
	verb   rune
	format func()
}

func vF(verb rune, format func()) verbFormatter {
	return verbFormatter{verb: verb, format: format}
}

func formatValue(v Value, s fmt.State, verb rune, other ...verbFormatter) {
	if verb == 'q' {
		fmt.Println()
	}
	for _, rf := range append(other,
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, ")")
			}
		}),
		vF('s', func() {
			_, _ = io.WriteString(s, v.String())
		}),
		vF('q', func() {
			_, _ = io.WriteString(s, v.String())
		}),
		vF('T', func() {
			_, _ = io.WriteString(s, v.Type().String())
		}),
	) {
		if rf.verb == verb {
			rf.format()
			return
		}
	}
	_, _ = io.WriteString(s,
		fmt.Sprintf("unknown formatter verb '%s' for value type '%s'", string(verb), v.Type().String()),
	)
}

func FromYDB(t *Ydb.Type, v *Ydb.Value) Value {
	if vv, err := fromYDB(t, v); err != nil {
		panic(err)
	} else {
		return vv
	}
}

func nullValueFromYDB(x *Ydb.Value, t Type) (_ Value, ok bool) {
	for {
		switch xx := x.Value.(type) {
		case *Ydb.Value_NestedValue:
			x = xx.NestedValue
		case *Ydb.Value_NullFlagValue:
			switch tt := t.(type) {
			case *optionalType:
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
			return YSONValue([]byte(vv.TextValue)), nil
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

	case *DecimalType:
		return DecimalValue(BigEndianUint128(v.High_128, v.GetLow_128()), ttt.Precision, ttt.Scale), nil

	case *optionalType:
		t = t.Type.(*Ydb.Type_OptionalType).OptionalType.Item
		if nestedValue, ok := v.Value.(*Ydb.Value_NestedValue); ok {
			return OptionalValue(FromYDB(t, nestedValue.NestedValue)), nil
		}
		return OptionalValue(FromYDB(t, v)), nil

	case *listType:
		return ListValue(func() (vv []Value) {
			a := allocator.New()
			defer a.Free()
			for _, vvv := range v.Items {
				vv = append(vv, FromYDB(ttt.itemType.toYDB(a), vvv))
			}
			return vv
		}()...), nil

	case *TupleType:
		return TupleValue(func() (vv []Value) {
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
					K: FromYDB(ttt.keyType.toYDB(a), vvv.Key),
					V: FromYDB(ttt.valueType.toYDB(a), vvv.Payload),
				})
			}
			return vv
		}()...), nil

	case *variantType:
		a := allocator.New()
		defer a.Free()
		switch ttt.variantType {
		case variantTypeTuple:
			return VariantValue(
				FromYDB(
					ttt.innerType.(*TupleType).items[v.VariantIndex].toYDB(a),
					v.Value.(*Ydb.Value_NestedValue).NestedValue,
				),
				v.VariantIndex,
				ttt.innerType,
			), nil
		case variantTypeStruct:
			return VariantValue(
				FromYDB(
					ttt.innerType.(*StructType).fields[v.VariantIndex].T.toYDB(a),
					v.Value.(*Ydb.Value_NestedValue).NestedValue,
				),
				v.VariantIndex,
				ttt.innerType,
			), nil
		default:
			return nil, fmt.Errorf("unknown variant type: %v", ttt.variantType)
		}

	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("uncovered type: %T", ttt))
	}
}

type boolValue bool

func (v boolValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v boolValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *bool:
		*vv = bool(v)
		return nil
	case *string:
		*vv = strconv.FormatBool(bool(v))
		return nil
	default:
		return fmt.Errorf("cannot cast '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v boolValue) String() string {
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

func (v dateValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb,
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(\"")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, "\")")
			}
		}),
		vF('q', func() {
			_, _ = io.WriteString(s, "\""+v.String()+"\"")
		}),
	)
}

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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v dateValue) String() string {
	return DateToTime(uint32(v)).Format(LayoutDate)
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
	return dateValue(uint64(t.Sub(unix)/time.Second) / secondsPerDay)
}

type datetimeValue uint32

func (v datetimeValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb,
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(\"")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, "\")")
			}
		}),
		vF('q', func() {
			_, _ = io.WriteString(s, "\""+v.String()+"\"")
		}),
	)
}

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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v datetimeValue) String() string {
	return DatetimeToTime(uint32(v)).Format(LayoutDatetime)
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
	return datetimeValue(uint64(t.Sub(unix) / time.Second))
}

type decimalValue struct {
	value     [16]byte
	innerType *DecimalType
}

func (v *decimalValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *decimalValue) castTo(dst interface{}) error {
	return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), dst)
}

func (v *decimalValue) String() string {
	return decimal.FromBytes(v.value[:], v.innerType.Precision, v.innerType.Scale).String()
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

func DecimalValue(v [16]byte, precision uint32, scale uint32) *decimalValue {
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

func (v *dictValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *dictValue) castTo(dst interface{}) error {
	return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), dst)
}

func (v *dictValue) String() string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteByte('{')
	for i, value := range v.values {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(fmt.Sprintf("%q", value.K))
		buffer.WriteByte(':')
		buffer.WriteString(fmt.Sprintf("%q", value.V))
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
	value float64
}

func (v *doubleValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('f', func() {
		precision := -1
		if p, hasPrecision := s.Precision(); hasPrecision {
			precision = p
		}
		_, _ = io.WriteString(s, strconv.FormatFloat(v.value, 'f', precision, 64))
	}))
}

func (v *doubleValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatFloat(v.value, 'f', -1, 64)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatFloat(v.value, 'f', -1, 64))
		return nil
	case *float64:
		*vv = v.value
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v *doubleValue) String() string {
	return fmt.Sprintf("%v", v.value)
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

type dyNumberValue struct {
	value string
}

func (v *dyNumberValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *dyNumberValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = v.value
		return nil
	case *[]byte:
		*vv = []byte(v.value)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v *dyNumberValue) String() string {
	return v.value
}

func (*dyNumberValue) Type() Type {
	return TypeDyNumber
}

func (v *dyNumberValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.value
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DyNumberValue(v string) *dyNumberValue {
	return &dyNumberValue{value: v}
}

type floatValue struct {
	value float32
}

func (v *floatValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('f', func() {
		precision := -1
		if p, hasPrecision := s.Precision(); hasPrecision {
			precision = p
		}
		_, _ = io.WriteString(s, strconv.FormatFloat(float64(v.value), 'f', precision, 32))
	}))
}

func (v *floatValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatFloat(float64(v.value), 'f', -1, 32)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatFloat(float64(v.value), 'f', -1, 32))
		return nil
	case *float64:
		*vv = float64(v.value)
		return nil
	case *float32:
		*vv = v.value
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v *floatValue) String() string {
	return fmt.Sprintf("%v", v.value)
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

func (v int8Value) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('d', func() {
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v int8Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatInt(int64(v), 10))
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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v int8Value) String() string {
	return strconv.FormatInt(int64(v), 10)
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

func (v int16Value) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('d', func() {
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v int16Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatInt(int64(v), 10))
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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v int16Value) String() string {
	return strconv.FormatInt(int64(v), 10)
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

func (v int32Value) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('d', func() {
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v int32Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatInt(int64(v), 10))
		return nil
	case *int64:
		*vv = int64(v)
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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v int32Value) String() string {
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

func (v int64Value) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('d', func() {
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v int64Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatInt(int64(v), 10))
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	case *float64:
		*vv = float64(v)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v int64Value) String() string {
	return strconv.FormatInt(int64(v), 10)
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

func (v intervalValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb,
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(\"")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, "\")")
			}
		}),
		vF('q', func() {
			_, _ = io.WriteString(s, "\""+v.String()+"\"")
		}),
	)
}

func (v intervalValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *time.Duration:
		*vv = IntervalToDuration(int64(v))
		return nil
	case *int64:
		*vv = int64(v)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v intervalValue) String() string {
	return IntervalToDuration(int64(v)).String()
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

type jsonValue struct {
	value string
}

func (v *jsonValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *jsonValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = v.value
		return nil
	case *[]byte:
		*vv = []byte(v.value)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v *jsonValue) String() string {
	return v.value
}

func (*jsonValue) Type() Type {
	return TypeJSON
}

func (v *jsonValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.value
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONValue(v string) *jsonValue {
	return &jsonValue{value: v}
}

type jsonDocumentValue struct {
	value string
}

func (v *jsonDocumentValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *jsonDocumentValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = v.value
		return nil
	case *[]byte:
		*vv = []byte(v.value)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v *jsonDocumentValue) String() string {
	return v.value
}

func (*jsonDocumentValue) Type() Type {
	return TypeJSONDocument
}

func (v *jsonDocumentValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.value
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func JSONDocumentValue(v string) *jsonDocumentValue {
	return &jsonDocumentValue{value: v}
}

type listValue struct {
	t     Type
	items []Value
}

func (v *listValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *listValue) castTo(dst interface{}) error {
	return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), dst)
}

func (v *listValue) String() string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteString("[")
	for i, item := range v.items {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(fmt.Sprintf("%q", item))
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

func (v *nullValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *nullValue) castTo(dst interface{}) error {
	return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), dst)
}

func (v *nullValue) String() string {
	return "NULL"
}

func (v *nullValue) Type() Type {
	return v.t
}

func (v *nullValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Value()
	vv.Value = a.NullFlag()

	x := v.t.innerType
	for {
		opt, ok := x.(*optionalType)
		if !ok {
			break
		}
		x = opt.innerType
		nestedValue := a.Nested()
		nestedValue.NestedValue = vv
		vv = a.Value()
		vv.Value = nestedValue
	}

	return vv
}

func NullValue(t Type) *nullValue {
	return &nullValue{
		t: Optional(t),
	}
}

type optionalValue struct {
	innerType Type
	value     Value
}

func (v *optionalValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *optionalValue) castTo(dst interface{}) error {
	return v.value.castTo(dst)
}

func (v *optionalValue) String() string {
	return v.value.String()
}

func (v *optionalValue) Type() Type {
	return v.innerType
}

func (v *optionalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	if _, opt := v.value.(*optionalValue); opt {
		vv := a.Nested()
		vv.NestedValue = v.value.toYDB(a)
		vvv.Value = vv
	} else {
		vvv.Value = v.value.toYDB(a).Value
	}

	return vvv
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

func (v *structValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *structValue) castTo(dst interface{}) error {
	return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), dst)
}

func (v *structValue) String() string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	a := allocator.New()
	defer a.Free()
	buffer.WriteString("{")
	for i, field := range v.fields {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteByte('"')
		buffer.WriteString(field.Name)
		buffer.WriteByte('"')
		buffer.WriteByte(':')
		buffer.WriteString(fmt.Sprintf("%q", field.V))
	}
	buffer.WriteByte('}')
	return buffer.String()
}

func (v *structValue) Type() Type {
	return v.t
}

func (v structValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vvv := a.Value()

	for _, field := range v.fields {
		vvv.Items = append(vvv.Items, field.V.toYDB(a))
	}

	return vvv
}

func StructValue(fields ...StructValueField) *structValue {
	structFields := make([]StructField, 0, len(fields))
	for _, field := range fields {
		structFields = append(structFields, StructField{field.Name, field.V.Type()})
	}
	return &structValue{
		t:      Struct(structFields...),
		fields: fields,
	}
}

type timestampValue uint64

func (v timestampValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb,
		vF('q', func() {
			_, _ = io.WriteString(s, "\""+v.String()+"\"")
		}),
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(\"")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, "\")")
			}
		}),
	)
}

func (v timestampValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *time.Time:
		*vv = TimestampToTime(uint64(v))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v timestampValue) String() string {
	return TimestampToTime(uint64(v)).Format(LayoutTimestamp)
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
	return timestampValue(t.Sub(unix) / time.Microsecond)
}

type tupleValue struct {
	t     Type
	items []Value
}

func (v *tupleValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('v', func() {
		if s.Flag('+') {
			_, _ = io.WriteString(s, v.Type().String())
		}
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v *tupleValue) castTo(dst interface{}) error {
	return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), dst)
}

func (v *tupleValue) String() string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteString("(")
	for i, item := range v.items {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(fmt.Sprintf("%q", item))
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

func (v tzDateValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb,
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(\"")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, "\")")
			}
		}),
		vF('q', func() {
			_, _ = io.WriteString(s, "\""+v.String()+"\"")
		}),
	)
}

func (v tzDateValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = []byte(v)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v tzDateValue) String() string {
	return string(v)
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
	return tzDateValue(t.Format(LayoutTzDate))
}

type tzDatetimeValue string

func (v tzDatetimeValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb,
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(\"")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, "\")")
			}
		}),
		vF('q', func() {
			_, _ = io.WriteString(s, "\""+v.String()+"\"")
		}),
	)
}

func (v tzDatetimeValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = []byte(v)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v tzDatetimeValue) String() string {
	return string(v)
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
	return tzDatetimeValue(t.Format(LayoutTzDatetime))
}

type tzTimestampValue string

func (v tzTimestampValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb,
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(\"")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, "\")")
			}
		}),
		vF('q', func() {
			_, _ = io.WriteString(s, "\""+v.String()+"\"")
		}),
	)
}

func (v tzTimestampValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = []byte(v)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v tzTimestampValue) String() string {
	return string(v)
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
	return tzTimestampValue(t.Format(LayoutTzTimestamp))
}

type uint8Value uint8

func (v uint8Value) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('d', func() {
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v uint8Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatInt(int64(v), 10))
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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v uint8Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
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

func (v uint16Value) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('d', func() {
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v uint16Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatInt(int64(v), 10))
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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v uint16Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
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

func (v uint32Value) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('d', func() {
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v uint32Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatInt(int64(v), 10))
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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v uint32Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
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

func (v uint64Value) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('d', func() {
		_, _ = io.WriteString(s, v.String())
	}))
}

func (v uint64Value) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = strconv.FormatInt(int64(v), 10)
		return nil
	case *[]byte:
		*vv = []byte(strconv.FormatInt(int64(v), 10))
		return nil
	case *uint64:
		*vv = uint64(v)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v uint64Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
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

type textValue struct {
	value string
}

func (v *textValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb,
		vF('q', func() {
			_, _ = io.WriteString(s, "\""+v.value+"\"")
		}),
		vF('v', func() {
			if s.Flag('+') {
				_, _ = io.WriteString(s, v.Type().String()+"(\"")
			}
			_, _ = io.WriteString(s, v.String())
			if s.Flag('+') {
				_, _ = io.WriteString(s, "\")")
			}
		}),
	)
}

func (v *textValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = v.value
		return nil
	case *[]byte:
		*vv = []byte(v.value)
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v *textValue) String() string {
	return v.value
}

func (*textValue) Type() Type {
	return TypeText
}

func (v *textValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.value
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TextValue(v string) *textValue {
	return &textValue{value: v}
}

type uuidValue struct {
	value [16]byte
}

func (v *uuidValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
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
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v *uuidValue) String() string {
	return "\"" + hex.EncodeToString(v.value[:]) + "\""
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

func (v *variantValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb, vF('v', func() {
		if s.Flag('+') {
			_, _ = io.WriteString(s, v.Type().String()+"(")
		}
		_, _ = io.WriteString(s, "{")
		_, _ = io.WriteString(s, strconv.FormatUint(uint64(v.idx), 10))
		_, _ = io.WriteString(s, ":")
		_, _ = io.WriteString(s, fmt.Sprintf("%q", v.value))
		_, _ = io.WriteString(s, "}")
		if s.Flag('+') {
			_, _ = io.WriteString(s, ")")
		}
	}))
}

func (v *variantValue) castTo(dst interface{}) error {
	return v.value.castTo(dst)
}

func (v *variantValue) String() string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteString("{")
	buffer.WriteString(strconv.FormatUint(uint64(v.idx), 10))
	buffer.WriteByte(':')
	buffer.WriteString(fmt.Sprintf("%q", v.value))
	buffer.WriteString("}")
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

func VariantValue(v Value, idx uint32, t Type) *variantValue {
	return &variantValue{
		innerType: Variant(t),
		value:     v,
		idx:       idx,
	}
}

func VariantValueStruct(v Value, idx uint32) *variantValue {
	if _, ok := v.(*structValue); !ok {
		panic("value must be a struct type")
	}
	return &variantValue{
		innerType: &variantType{
			innerType:   v.Type(),
			variantType: variantTypeStruct,
		},
		value: v,
		idx:   idx,
	}
}

func VariantValueTuple(v Value, idx uint32) *variantValue {
	if _, ok := v.(*tupleValue); !ok {
		panic("value must be a tuple type")
	}
	return &variantValue{
		innerType: &variantType{
			innerType:   v.Type(),
			variantType: variantTypeTuple,
		},
		value: v,
		idx:   idx,
	}
}

type voidValue struct{}

func (v voidValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v voidValue) castTo(dst interface{}) error {
	return fmt.Errorf("cannot cast '%s' to '%T'", v.Type().String(), dst)
}

func (v voidValue) String() string {
	return "VOID"
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

type ysonValue struct {
	value []byte
}

func (v *ysonValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v *ysonValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v.value)
		return nil
	case *[]byte:
		*vv = v.value
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v *ysonValue) String() string {
	return string(v.value)
}

func (*ysonValue) Type() Type {
	return TypeYSON
}

func (v *ysonValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Bytes()
	if v != nil {
		vv.BytesValue = v.value
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func YSONValue(v []byte) *ysonValue {
	return &ysonValue{value: v}
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

	case *optionalType:
		return NullValue(t.innerType)

	case *voidType:
		return VoidValue()

	case *listType:
		return &listValue{
			t: t,
		}
	case *TupleType:
		v := &tupleValue{
			t:     t,
			items: make([]Value, len(t.items)),
		}
		for i, tt := range t.items {
			v.items[i] = ZeroValue(tt)
		}
		return v
	case *StructType:
		v := &structValue{
			t:      t,
			fields: make([]StructValueField, len(t.fields)),
		}
		for i, tt := range t.fields {
			v.fields[i] = StructValueField{
				Name: tt.Name,
				V:    ZeroValue(tt.T),
			}
		}
		return v
	case *dictType:
		return &dictValue{
			t: t,
		}
	case *DecimalType:
		return DecimalValue([16]byte{}, 22, 9)

	case *variantType:
		return VariantValue(ZeroValue(t.innerType), 0, t.innerType)

	default:
		panic(fmt.Sprintf("uncovered type '%T'", t))
	}
}

type bytesValue []byte

func (v bytesValue) Format(s fmt.State, verb rune) {
	formatValue(v, s, verb)
}

func (v bytesValue) castTo(dst interface{}) error {
	switch vv := dst.(type) {
	case *string:
		*vv = string(v)
		return nil
	case *[]byte:
		*vv = v
		return nil
	default:
		return fmt.Errorf("cannot cast YDB type '%s' to '%T'", v.Type().String(), vv)
	}
}

func (v bytesValue) String() string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	buffer.WriteByte('[')
	for i, b := range []byte(v) {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString("0x")
		buffer.WriteString(strings.ToUpper(strconv.FormatUint(uint64(b), 16)))
	}
	buffer.WriteByte(']')
	return buffer.String()
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
