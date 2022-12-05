package logs

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

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
// Getter methods must not be called on fields with wrong Type (e.g. calling String()logs.$1
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

// String is a value getter for fields with StringType type
func (f Field) String() string {
	return f.vstr
}

// Int is a value getter for fields with IntType type
func (f Field) Int() int {
	return int(f.vint)
}

// Bool is a value getter for fields with BoolType type
func (f Field) Bool() bool {
	return f.vint != 0
}

// Duration is a value getter for fields with DurationType type
func (f Field) Duration() time.Duration {
	return time.Nanosecond * time.Duration(f.vint)
}

// Strings is a value getter for fields with StringsType type
func (f Field) Strings() []string {
	if f.vany == nil {
		return nil
	}
	return f.vany.([]string)
}

// Error is a value getter for fields with ErrorType type
func (f Field) Error() error {
	if f.vany == nil {
		return nil
	}
	return f.vany.(error)
}

// Any is a value getter for fields with AnyType type
func (f Field) Any() interface{} {
	return f.vany
}

// Stringer is a value getteer for fields with StringerType type
func (f Field) Stringer() fmt.Stringer {
	if f.vany == nil {
		return nil
	}
	return f.vany.(fmt.Stringer)
}

// Endpoints is a value getter for fields with EndpointsType type
func (f Field) Endpoints() []trace.EndpointInfo {
	if f.vany == nil {
		return nil
	}
	return f.vany.([]trace.EndpointInfo)
}

// Metadata is a value getter for fields with MetadataType type
func (f Field) Metadata() map[string][]string {
	if f.vany == nil {
		return nil
	}
	return f.vany.(map[string][]string)
}

// Fallback returns default string representation of Field value.
// It should be used by adapters that don't support f.Type directly.
// Non-nil error indicates some fatal marshaling error and is most
// likely a sign of invalid Field initialization.
func (f Field) Fallback() (repr string, err error) {
	func() {
		defer func() {
			if p := recover(); p != nil {
				if e, ok := p.(error); ok {
					err = e
				} else {
					err = fmt.Errorf("%v", p)
				}
			}
		}()
		switch f.ftype {
		case IntType:
			repr = strconv.FormatInt(f.vint, 10)
		case StringType:
			repr = f.vstr
		case BoolType:
			repr = strconv.FormatBool(f.Bool())
		case DurationType:
			repr = f.Duration().String()
		case StringsType:
			repr = fmt.Sprintf("%v", f.Strings())
		case ErrorType:
			repr = f.Error().Error()
		case AnyType:
			repr = fmt.Sprint(f.vany)
		case NilType:
			repr = "<nil>"
		case StringerType:
			repr = f.Stringer().String()
		case EndpointsType:
			repr = fmt.Sprintf("%v", f.Endpoints())
		case MetadataType:
			repr = fmt.Sprintf("%v", f.Metadata())
		default:
			err = fmt.Errorf("unknown FieldType %d", f.ftype)
		}
	}()
	return repr, err
}

// String constructs Field with StringType
func String(key string, value string) Field {
	return Field{
		ftype: StringType,
		key:   key,
		vstr:  value,
	}
}

// Int constructs Field with IntType
func Int(key string, value int) Field {
	return Field{
		ftype: IntType,
		key:   key,
		vint:  int64(value),
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

// NamedError constructs Field with ErrorType. If value is nil,
// resulting Field will be of NilType instead of ErrorType.
func NamedError(key string, value error) Field {
	if value == nil {
		return nilField(key)
	}
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

// Any constructs untyped Field. If value is nil, resulting Field
// will be of NilType instead of AnyType.
func Any(key string, value interface{}) Field {
	if value == nil {
		return nilField(key)
	}
	return Field{
		ftype: AnyType,
		key:   key,
		vany:  value,
	}
}

func nilField(key string) Field {
	return Field{
		ftype: NilType,
		key:   key,
	}
}

// Stringer constructs Field with StringerType. If value is nil,
// resulting Field will be of NilType instead of StringerType.
func Stringer(key string, value fmt.Stringer) Field {
	if value == nil {
		return nilField(key)
	}
	return Field{
		ftype: StringerType,
		key:   key,
		vany:  value,
	}
}

// Endpoints constructs Field with EndpointsType
func Endpoints(key string, value []trace.EndpointInfo) Field {
	return Field{
		ftype: EndpointsType,
		key:   key,
		vany:  value,
	}
}

// Metadata constructs Field with MetadataType
func Metadata(key string, value map[string][]string) Field {
	return Field{
		ftype: MetadataType,
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
	StringType
	BoolType
	DurationType

	// StringsType corresponds to []string
	StringsType

	ErrorType
	// AnyType indicates that the Field is untyped. Adapters should use
	// reflection-based approached to marshal this field.
	AnyType
	// NilType indicates that the Field value is nil. No value getters should
	// be called on field with such type.
	NilType

	// StringerType corresponds to fmt.Stringer
	StringerType

	// EndpointsType corresponds to []trace.EndpointInfo
	EndpointsType
	// MetadataType corresponds to map[string][]string
	MetadataType
)
