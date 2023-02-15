package types

import (
	"bytes"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

// Type describes YDB data types.
type Type = value.Type

// Equal checks for type equivalence
func Equal(lhs, rhs Type) bool {
	return value.TypesEqual(lhs, rhs)
}

func List(t Type) Type {
	return value.List(t)
}

func Tuple(elems ...Type) Type {
	return value.Tuple(elems...)
}

type tStructType struct {
	fields []value.StructField
}

type StructOption func(*tStructType)

func StructField(name string, t Type) StructOption {
	return func(s *tStructType) {
		s.fields = append(s.fields, value.StructField{
			Name: name,
			T:    t,
		})
	}
}

func Struct(opts ...StructOption) Type {
	var s tStructType
	for _, opt := range opts {
		if opt != nil {
			opt(&s)
		}
	}
	return value.Struct(s.fields...)
}

func Dict(k, v Type) Type {
	return value.Dict(k, v)
}

func VariantStruct(opts ...StructOption) Type {
	var s tStructType
	for _, opt := range opts {
		if opt != nil {
			opt(&s)
		}
	}
	return value.VariantStruct(s.fields...)
}

func VariantTuple(elems ...Type) Type {
	return value.VariantTuple(elems...)
}

func Void() Type {
	return value.Void()
}

func Optional(t Type) Type {
	return value.Optional(t)
}

var DefaultDecimal = DecimalType(22, 9)

func DecimalType(precision, scale uint32) Type {
	return value.Decimal(precision, scale)
}

func DecimalTypeFromDecimal(d *Decimal) Type {
	return value.Decimal(d.Precision, d.Scale)
}

// Primitive types known by YDB.
const (
	TypeUnknown      = value.TypeUnknown
	TypeBool         = value.TypeBool
	TypeInt8         = value.TypeInt8
	TypeUint8        = value.TypeUint8
	TypeInt16        = value.TypeInt16
	TypeUint16       = value.TypeUint16
	TypeInt32        = value.TypeInt32
	TypeUint32       = value.TypeUint32
	TypeInt64        = value.TypeInt64
	TypeUint64       = value.TypeUint64
	TypeFloat        = value.TypeFloat
	TypeDouble       = value.TypeDouble
	TypeDate         = value.TypeDate
	TypeDatetime     = value.TypeDatetime
	TypeTimestamp    = value.TypeTimestamp
	TypeInterval     = value.TypeInterval
	TypeTzDate       = value.TypeTzDate
	TypeTzDatetime   = value.TypeTzDatetime
	TypeTzTimestamp  = value.TypeTzTimestamp
	TypeString       = value.TypeBytes
	TypeBytes        = value.TypeBytes
	TypeUTF8         = value.TypeText
	TypeText         = value.TypeText
	TypeYSON         = value.TypeYSON
	TypeJSON         = value.TypeJSON
	TypeUUID         = value.TypeUUID
	TypeJSONDocument = value.TypeJSONDocument
	TypeDyNumber     = value.TypeDyNumber
)

// WriteTypeStringTo writes ydb type string representation into buffer
//
// Deprecated: use types.Type.Yql() instead
func WriteTypeStringTo(buf *bytes.Buffer, t Type) {
	buf.WriteString(t.Yql())
}

// RawValue scanning non-primitive yql types or for own implementation scanner native API
type RawValue interface {
	Path() string
	WritePathTo(w io.Writer) (n int64, err error)
	Type() Type
	Bool() (v bool)
	Int8() (v int8)
	Uint8() (v uint8)
	Int16() (v int16)
	Uint16() (v uint16)
	Int32() (v int32)
	Uint32() (v uint32)
	Int64() (v int64)
	Uint64() (v uint64)
	Float() (v float32)
	Double() (v float64)
	Date() (v time.Time)
	Datetime() (v time.Time)
	Timestamp() (v time.Time)
	Interval() (v time.Duration)
	TzDate() (v time.Time)
	TzDatetime() (v time.Time)
	TzTimestamp() (v time.Time)
	String() (v []byte)
	UTF8() (v string)
	YSON() (v []byte)
	JSON() (v []byte)
	UUID() (v [16]byte)
	JSONDocument() (v []byte)
	DyNumber() (v string)
	Value() Value

	// Any returns any primitive or optional value.
	// Currently, it may return one of these types:
	//
	//   bool
	//   int8
	//   uint8
	//   int16
	//   uint16
	//   int32
	//   uint32
	//   int64
	//   uint64
	//   float32
	//   float64
	//   []byte
	//   string
	//   [16]byte
	//
	Any() interface{}

	// Unwrap unwraps current item under scan interpreting it as Optional<Type> types.
	Unwrap()
	AssertType(t Type) bool
	IsNull() bool
	IsOptional() bool

	// ListIn interprets current item under scan as a ydb's list.
	// It returns the size of the nested items.
	// If current item under scan is not a list types, it returns -1.
	ListIn() (size int)

	// ListItem selects current item i-th element as an item to scan.
	// ListIn() must be called before.
	ListItem(i int)

	// ListOut leaves list entered before by ListIn() call.
	ListOut()

	// TupleIn interprets current item under scan as a ydb's tuple.
	// It returns the size of the nested items.
	TupleIn() (size int)

	// TupleItem selects current item i-th element as an item to scan.
	// Note that TupleIn() must be called before.
	// It panics if it is out of bounds.
	TupleItem(i int)

	// TupleOut leaves tuple entered before by TupleIn() call.
	TupleOut()

	// StructIn interprets current item under scan as a ydb's struct.
	// It returns the size of the nested items – the struct fields values.
	// If there is no current item under scan it returns -1.
	StructIn() (size int)

	// StructField selects current item i-th field value as an item to scan.
	// Note that StructIn() must be called before.
	// It panics if i is out of bounds.
	StructField(i int) (name string)

	// StructOut leaves struct entered before by StructIn() call.
	StructOut()

	// DictIn interprets current item under scan as a ydb's dict.
	// It returns the size of the nested items pairs.
	// If there is no current item under scan it returns -1.
	DictIn() (size int)

	// DictKey selects current item i-th pair key as an item to scan.
	// Note that DictIn() must be called before.
	// It panics if i is out of bounds.
	DictKey(i int)

	// DictPayload selects current item i-th pair value as an item to scan.
	// Note that DictIn() must be called before.
	// It panics if i is out of bounds.
	DictPayload(i int)

	// DictOut leaves dict entered before by DictIn() call.
	DictOut()

	// Variant unwraps current item under scan interpreting it as Variant<Type> types.
	// It returns non-empty name of a field that is filled for struct-based
	// variant.
	// It always returns an index of filled field of a Type.
	Variant() (name string, index uint32)

	// Decimal returns decimal value represented by big-endian 128 bit signed integer.
	Decimal(t Type) (v [16]byte)

	// UnwrapDecimal returns decimal value represented by big-endian 128 bit signed
	// integer and its types information.
	UnwrapDecimal() Decimal
	IsDecimal() bool
	Err() error
}

// Scanner scanning raw ydb types
type Scanner interface {
	// UnmarshalYDB must be implemented on client-side for unmarshal raw ydb value.
	UnmarshalYDB(raw RawValue) error
}
