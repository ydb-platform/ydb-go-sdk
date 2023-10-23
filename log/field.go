package log

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const nilPtr = "<nil>"

// Field represents typed log field (a key-value pair). Adapters should determine
// Field's type based on Type and use the corresponding getter method to retrieve
// the value:
//
//	switch f.Type() {
//	case logs.IntType:
//	    var i int = f.Int()
//	    // handle int value
//	case logs.StringType:
//	    var s string = f.String()
//	    // handle string value
//	//...
//	}
//
// Getter methods must not be called on fields with wrong Type (e.g. calling String()
// on fields with Type != StringType).
// Field must not be initialized directly as a struct literal.
type Field struct {
	ftype FieldType
	key   string

	vint int64
	vstr string
	vany interface{}
}

func (f Field) Type() FieldType {
	return f.ftype
}

func (f Field) Key() string {
	return f.key
}

// StringValue is a value getter for fields with StringType type
func (f Field) StringValue() string {
	f.checkType(StringType)
	return f.vstr
}

// IntValue is a value getter for fields with IntType type
func (f Field) IntValue() int {
	f.checkType(IntType)
	return int(f.vint)
}

// Int64Value is a value getter for fields with Int64Type type
func (f Field) Int64Value() int64 {
	f.checkType(Int64Type)
	return f.vint
}

// BoolValue is a value getter for fields with BoolType type
func (f Field) BoolValue() bool {
	f.checkType(BoolType)
	return f.vint != 0
}

// DurationValue is a value getter for fields with DurationType type
func (f Field) DurationValue() time.Duration {
	f.checkType(DurationType)
	return time.Nanosecond * time.Duration(f.vint)
}

// StringsValue is a value getter for fields with StringsType type
func (f Field) StringsValue() []string {
	f.checkType(StringsType)
	if f.vany == nil {
		return nil
	}
	return f.vany.([]string)
}

// ErrorValue is a value getter for fields with ErrorType type
func (f Field) ErrorValue() error {
	f.checkType(ErrorType)
	if f.vany == nil {
		return nil
	}
	return f.vany.(error)
}

// AnyValue is a value getter for fields with AnyType type
func (f Field) AnyValue() interface{} {
	switch f.ftype {
	case AnyType:
		return f.vany
	case IntType:
		return f.IntValue()
	case Int64Type:
		return f.Int64Value()
	case StringType:
		return f.StringValue()
	case BoolType:
		return f.BoolValue()
	case DurationType:
		return f.DurationValue()
	case StringsType:
		return f.StringsValue()
	case ErrorType:
		return f.ErrorValue()
	case StringerType:
		return f.Stringer()
	default:
		panic(fmt.Sprintf("unknown FieldType %d", f.ftype))
	}
}

// Stringer is a value getter for fields with StringerType type
func (f Field) Stringer() fmt.Stringer {
	f.checkType(StringerType)
	if f.vany == nil {
		return nil
	}
	return f.vany.(fmt.Stringer)
}

// Panics on type mismatch
func (f Field) checkType(want FieldType) {
	if f.ftype != want {
		panic(fmt.Sprintf("bad type. have: %s, want: %s", f.ftype, want))
	}
}

// Returns default string representation of Field value.
// It should be used by adapters that don't support f.Type directly.
func (f Field) String() string {
	switch f.ftype {
	case IntType, Int64Type:
		return strconv.FormatInt(f.vint, 10)
	case StringType:
		return f.vstr
	case BoolType:
		return strconv.FormatBool(f.BoolValue())
	case DurationType:
		return f.DurationValue().String()
	case StringsType:
		return fmt.Sprintf("%v", f.StringsValue())
	case ErrorType:
		if f.vany == nil || f.vany.(error) == nil {
			return "<nil>"
		}
		return f.ErrorValue().Error()
	case AnyType:
		if f.vany == nil {
			return nilPtr
		}
		if v := reflect.ValueOf(f.vany); v.Type().Kind() == reflect.Ptr {
			if v.IsNil() {
				return nilPtr
			}
			return v.Type().String() + "(" + fmt.Sprint(v.Elem()) + ")"
		}
		return fmt.Sprint(f.vany)
	case StringerType:
		return f.Stringer().String()
	default:
		panic(fmt.Sprintf("unknown FieldType %d", f.ftype))
	}
}

// String constructs Field with StringType
func String(k, v string) Field {
	return Field{
		ftype: StringType,
		key:   k,
		vstr:  v,
	}
}

// Int constructs Field with IntType
func Int(k string, v int) Field {
	return Field{
		ftype: IntType,
		key:   k,
		vint:  int64(v),
	}
}

func Int64(k string, v int64) Field {
	return Field{
		ftype: Int64Type,
		key:   k,
		vint:  v,
	}
}

// Bool constructs Field with BoolType
func Bool(key string, value bool) Field {
	var vint int64
	if value {
		vint = 1
	} else {
		vint = 0
	}
	return Field{
		ftype: BoolType,
		key:   key,
		vint:  vint,
	}
}

// Duration constructs Field with DurationType
func Duration(key string, value time.Duration) Field {
	return Field{
		ftype: DurationType,
		key:   key,
		vint:  value.Nanoseconds(),
	}
}

// Strings constructs Field with StringsType
func Strings(key string, value []string) Field {
	return Field{
		ftype: StringsType,
		key:   key,
		vany:  value,
	}
}

// NamedError constructs Field with ErrorType
func NamedError(key string, value error) Field {
	return Field{
		ftype: ErrorType,
		key:   key,
		vany:  value,
	}
}

// Error is the same as NamedError("error", value)
func Error(value error) Field {
	return NamedError("error", value)
}

// Any constructs untyped Field.
func Any(key string, value interface{}) Field {
	return Field{
		ftype: AnyType,
		key:   key,
		vany:  value,
	}
}

// Stringer constructs Field with StringerType. If value is nil,
// resulting Field will be of AnyType instead of StringerType.
func Stringer(key string, value fmt.Stringer) Field {
	if value == nil {
		return Any(key, nil)
	}
	return Field{
		ftype: StringerType,
		key:   key,
		vany:  value,
	}
}

// FieldType indicates type info about the Field. This enum might be extended in future releases.
// Adapters that don't support some FieldType value should use Field.Fallback() for marshaling.
type FieldType int

const (
	// InvalidType indicates that Field was not initialized correctly. Adapters
	// should either ignore such field or issue an error. No value getters should
	// be called on field with such type.
	InvalidType FieldType = iota

	IntType
	Int64Type
	StringType
	BoolType
	DurationType

	// StringsType corresponds to []string
	StringsType

	ErrorType
	// AnyType indicates that the Field is untyped. Adapters should use
	// reflection-based approached to marshal this field.
	AnyType

	// StringerType corresponds to fmt.Stringer
	StringerType

	endType
)

func (ft FieldType) String() (typeName string) {
	switch ft {
	case InvalidType:
		typeName = "invalid"
	case IntType:
		typeName = "int"
	case Int64Type:
		typeName = "int64"
	case StringType:
		typeName = "string"
	case BoolType:
		typeName = "bool"
	case DurationType:
		typeName = "time.Duration"
	case StringsType:
		typeName = "[]string"
	case ErrorType:
		typeName = "error"
	case AnyType:
		typeName = "any"
	case StringerType:
		typeName = "stringer"
	case endType:
		typeName = "endtype"
	default:
		panic("not implemented")
	}
	return typeName
}

// latencyField creates Field "latency": time.Since(start)
func latencyField(start time.Time) Field {
	return Duration("latency", time.Since(start))
}

// versionField creates Field "version": version.Version
func versionField() Field {
	return String("version", version.Version)
}

type endpoints []trace.EndpointInfo

func (ee endpoints) String() string {
	b := xstring.Buffer()
	defer b.Free()
	b.WriteByte('[')
	for i, e := range ee {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(e.String())
	}
	b.WriteByte(']')
	return b.String()
}

type metadata map[string][]string

func (m metadata) String() string {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf("error:%s", err)
	}
	return xstring.FromBytes(b)
}

func appendFieldByCondition(condition bool, ifTrueField Field, fields ...Field) []Field {
	if condition {
		fields = append(fields, ifTrueField)
	}
	return fields
}
