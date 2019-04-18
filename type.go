package ydb

import (
	"fmt"

	"github.com/yandex-cloud/ydb-go-sdk/internal"
)

// Type describes YDB data type.
type Type interface {
	internal.T
}

func List(T Type) Type {
	return internal.ListType{T: T}
}

func Tuple(elems ...Type) Type {
	es := make([]internal.T, len(elems))
	for i, el := range elems {
		es[i] = el
	}
	return internal.TupleType{
		Elems: es,
	}
}

type tStructType internal.StructType

type StructOption func(*tStructType)

func StructField(name string, typ Type) StructOption {
	return func(s *tStructType) {
		s.Fields = append(s.Fields, internal.StructField{
			Name: name,
			Type: typ,
		})
	}
}

func Struct(opts ...StructOption) Type {
	var s tStructType
	for _, opt := range opts {
		opt(&s)
	}
	return internal.StructType(s)
}

func Variant(x Type) Type {
	switch v := x.(type) {
	case internal.TupleType:
		return internal.VariantType{
			T: v,
		}
	case internal.StructType:
		return internal.VariantType{
			S: v,
		}
	default:
		panic(fmt.Sprintf("unsupported type for variant: %s", v))
	}
}

func Void() Type {
	return internal.VoidType{}
}

func Optional(T Type) Type {
	return internal.OptionalType{T: T}
}

var DefaultDecimal = Decimal(22, 9)

func Decimal(precision, scale uint32) Type {
	return internal.DecimalType{
		Precision: precision,
		Scale:     scale,
	}
}

// Primitive types known by YDB.
const (
	TypeUnknown     = internal.TypeUnknown
	TypeBool        = internal.TypeBool
	TypeInt8        = internal.TypeInt8
	TypeUint8       = internal.TypeUint8
	TypeInt16       = internal.TypeInt16
	TypeUint16      = internal.TypeUint16
	TypeInt32       = internal.TypeInt32
	TypeUint32      = internal.TypeUint32
	TypeInt64       = internal.TypeInt64
	TypeUint64      = internal.TypeUint64
	TypeFloat       = internal.TypeFloat
	TypeDouble      = internal.TypeDouble
	TypeDate        = internal.TypeDate
	TypeDatetime    = internal.TypeDatetime
	TypeTimestamp   = internal.TypeTimestamp
	TypeInterval    = internal.TypeInterval
	TypeTzDate      = internal.TypeTzDate
	TypeTzDatetime  = internal.TypeTzDatetime
	TypeTzTimestamp = internal.TypeTzTimestamp
	TypeString      = internal.TypeString
	TypeUTF8        = internal.TypeUTF8
	TypeYSON        = internal.TypeYSON
	TypeJSON        = internal.TypeJSON
	TypeUUID        = internal.TypeUUID
)
