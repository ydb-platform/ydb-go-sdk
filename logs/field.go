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
//	switch f.Type {
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
type Field struct {
	Type FieldType
	Key  string

	vint int64
	vstr string
	vany interface{}
}

func String(key string, value string) Field {
	return Field{
		Type: StringType,
		Key:  key,
		vstr: value,
	}
}

func (f Field) String() string {
	return f.vstr
}

func Int(key string, value int) Field {
	return Field{
		Type: IntType,
		Key:  key,
		vint: int64(value),
	}
}

func (f Field) Int() int {
	return int(f.vint)
}

func Bool(key string, value bool) Field {
	var vint int64
	if value {
		vint = 1
	} else {
		vint = 0
	}
	return Field{
		Type: BoolType,
		Key:  key,
		vint: vint,
	}
}

func (f Field) Bool() bool {
	return f.vint != 0
}

func Duration(key string, value time.Duration) Field {
	return Field{
		Type: DurationType,
		Key:  key,
		vint: value.Nanoseconds(),
	}
}

func (f Field) Duration() time.Duration {
	return time.Nanosecond * time.Duration(f.vint)
}

func Strings(key string, value []string) Field {
	return Field{
		Type: StringsType,
		Key:  key,
		vany: value,
	}
}

func (f Field) Strings() []string {
	return f.vany.([]string)
}

// NamedError adds error field. If value is nil, resulting Field will be
// of NilType instead of ErrorType.
func NamedError(key string, value error) Field {
	if value == nil {
		return nilField(key)
	}
	return Field{
		Type: ErrorType,
		Key:  key,
		vany: value,
	}
}

// Error is the same as NamedError("error", value)
func Error(value error) Field {
	return NamedError("error", value)
}

func (f Field) Error() error {
	return f.vany.(error)
}

// Any adds untyped field. If value is nil, resulting Field will be
// of NilType instead of AnyType.
func Any(key string, value interface{}) Field {
	if value == nil {
		return nilField(key)
	}
	return Field{
		Type: AnyType,
		Key:  key,
		vany: value,
	}
}

func (f Field) Any() interface{} {
	return f.vany
}

func nilField(key string) Field {
	return Field{
		Type: NilType,
		Key:  key,
	}
}

func Endpoints(key string, value []trace.EndpointInfo) Field {
	return Field{
		Type: EndpointsType,
		Key:  key,
		vany: value,
	}
}

func (f Field) Endpoints() []trace.EndpointInfo {
	return f.vany.([]trace.EndpointInfo)
}

// Fallback returns default string representation of Field value.
// It should be used by adapters that don't support f.Type directly.
func (f Field) Fallback() string {
	switch f.Type {
	case IntType:
		return strconv.FormatInt(f.vint, 10)
	case StringType:
		return f.vstr
	case BoolType:
		return strconv.FormatBool(f.Bool())
	case DurationType:
		return f.Duration().String()
	case StringsType:
		return fmt.Sprintf("%v", f.Strings())
	case ErrorType:
		return f.Error().Error()
	case AnyType:
		return fmt.Sprint(f.vany)
	case NilType:
		return "<nil>"
	case EndpointsType:
		return fmt.Sprintf("%v", f.Endpoints())
	default:
		panic(fmt.Sprintf("ydb: unknown FieldType %d", f.Type))
	}
}

// FieldType indicates type info about the Field. This enum might be extended in future releases.
// Adapters that don't support some FieldType value should use Field.Fallback() for marshaling.
type FieldType int

const (
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
	// NilType indicates that the Field value is nil.
	NilType

	// EndpointsType corresponds to []trace.EndpointInfo
	EndpointsType
)
