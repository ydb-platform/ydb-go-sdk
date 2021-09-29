/*
Package ydbtypes provides tools for integration ydb types with go/types package.
It is intended primarily for writing generation tools and linters.
*/
package ydbtypes

import (
	"fmt"
	"go/types"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func PrimitiveTypeFromString(s string) (t value.PrimitiveType, err error) {
	switch strings.ToLower(s) {
	case "bool":
		t = value.TypeBool
	case "int8":
		t = value.TypeInt8
	case "uint8":
		t = value.TypeUint8
	case "int16":
		t = value.TypeInt16
	case "uint16":
		t = value.TypeUint16
	case "int32":
		t = value.TypeInt32
	case "uint32":
		t = value.TypeUint32
	case "int64":
		t = value.TypeInt64
	case "uint64":
		t = value.TypeUint64
	case "float":
		t = value.TypeFloat
	case "double":
		t = value.TypeDouble
	case "date":
		t = value.TypeDate
	case "datetime":
		t = value.TypeDatetime
	case "timestamp":
		t = value.TypeTimestamp
	case "interval":
		t = value.TypeInterval
	case "tzdate":
		t = value.TypeTzDate
	case "tzdatetime":
		t = value.TypeTzDatetime
	case "tztimestamp":
		t = value.TypeTzTimestamp
	case "string":
		t = value.TypeString
	case "utf8":
		t = value.TypeUTF8
	case "yson":
		t = value.TypeYSON
	case "json":
		t = value.TypeJSON
	case "uuid":
		t = value.TypeUUID
	case "jsondocument":
		t = value.TypeJSONDocument
	case "dynumber":
		t = value.TypeDyNumber
	default:
		err = fmt.Errorf("ydbtypes: unknown types: %q", s)
	}
	return
}

func PrimitiveTypeFromGoType(typ types.Type) (t value.PrimitiveType, err error) {
	switch t := typ.(type) {
	case *types.Slice:
		b, basic := t.Elem().(*types.Basic)
		if basic && b.Kind() == types.Byte {
			return value.TypeString, nil
		}

	case *types.Basic:
		switch t.Kind() {
		case types.Bool:
			return value.TypeBool, nil
		case types.Int8:
			return value.TypeInt8, nil
		case types.Int16:
			return value.TypeInt16, nil
		case types.Int32:
			return value.TypeInt32, nil
		case types.Int64:
			return value.TypeInt64, nil
		case types.Uint8:
			return value.TypeUint8, nil
		case types.Uint16:
			return value.TypeUint16, nil
		case types.Uint32:
			return value.TypeUint32, nil
		case types.Uint64:
			return value.TypeUint64, nil
		case types.Float32:
			return value.TypeFloat, nil
		case types.Float64:
			return value.TypeDouble, nil
		case types.String:
			return value.TypeUTF8, nil

		case types.Int, types.Uint:
			return 0, fmt.Errorf(
				"ydbtypes: ambiguous mapping for go %s types exists",
				typ,
			)
		}
	}
	return 0, fmt.Errorf("ydbtypes: unsupported types: %s", typ)
}

var (
	bytesArray16 = types.NewArray(types.Typ[types.Byte], 16)
	bytesSlice   = types.NewSlice(types.Typ[types.Byte])
)

func GoTypeFromPrimitiveType(t value.PrimitiveType) types.Type {
	switch t {
	case value.TypeBool:
		return types.Typ[types.Bool]

	case value.TypeInt8:
		return types.Typ[types.Int8]

	case value.TypeUint8:
		return types.Typ[types.Uint8]

	case value.TypeInt16:
		return types.Typ[types.Int16]

	case value.TypeUint16:
		return types.Typ[types.Uint16]

	case value.TypeInt32:
		return types.Typ[types.Int32]

	case value.TypeUint32:
		return types.Typ[types.Uint32]

	case value.TypeInt64:
		return types.Typ[types.Int64]

	case value.TypeUint64:
		return types.Typ[types.Uint64]

	case value.TypeFloat:
		return types.Typ[types.Float32]

	case value.TypeDouble:
		return types.Typ[types.Float64]

	case value.TypeDate:
		return types.Typ[types.Uint32]

	case value.TypeDatetime:
		return types.Typ[types.Uint32]

	case value.TypeTimestamp:
		return types.Typ[types.Uint64]

	case value.TypeInterval:
		return types.Typ[types.Int64]

	case value.TypeTzDate:
		return types.Typ[types.String]

	case value.TypeTzDatetime:
		return types.Typ[types.String]

	case value.TypeTzTimestamp:
		return types.Typ[types.String]

	case value.TypeUTF8:
		return types.Typ[types.String]

	case value.TypeYSON:
		return types.Typ[types.String]

	case value.TypeJSON:
		return types.Typ[types.String]

	case value.TypeUUID:
		return bytesArray16

	case value.TypeJSONDocument:
		return types.Typ[types.String]

	case value.TypeDyNumber:
		return types.Typ[types.String]

	case value.TypeString:
		return bytesSlice

	default:
		panic("uncovered primitive types")
	}
}
