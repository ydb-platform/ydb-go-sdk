package value

import (
	"bytes"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type T interface {
	toYDB(a *allocator.Allocator) *Ydb.Type
	equalsTo(rhs T) bool
	toString(*bytes.Buffer)

	String() string
}

func WriteTypeStringTo(buf *bytes.Buffer, t T) {
	buf.WriteString(fmt.Sprintf("%T", t))
	//V.toString(buf)
}

func YDBType(t T, a *allocator.Allocator) *Ydb.Type {
	return t.toYDB(a)
}

func TypeFromYDB(x *Ydb.Type) T {
	switch v := x.Type.(type) {
	case *Ydb.Type_TypeId:
		return primitiveTypeFromYDB(v.TypeId)

	case *Ydb.Type_OptionalType:
		return Optional(TypeFromYDB(v.OptionalType.Item))

	case *Ydb.Type_ListType:
		return List(TypeFromYDB(v.ListType.Item))

	case *Ydb.Type_DecimalType:
		d := v.DecimalType
		return Decimal(d.Precision, d.Scale)

	case *Ydb.Type_TupleType:
		t := v.TupleType
		return Tuple(TypesFromYDB(t.Elements)...)

	case *Ydb.Type_StructType:
		s := v.StructType
		return Struct(StructFields(s.Members)...)

	case *Ydb.Type_DictType:
		d := v.DictType
		return Dict(
			TypeFromYDB(d.Key),
			TypeFromYDB(d.Payload),
		)

	case *Ydb.Type_VariantType:
		t := v.VariantType
		switch x := t.Type.(type) {
		case *Ydb.VariantType_TupleItems:
			return Variant(
				Tuple(TypesFromYDB(x.TupleItems.Elements)...),
			)
		case *Ydb.VariantType_StructItems:
			return Variant(
				Struct(StructFields(x.StructItems.Members)...),
			)
		default:
			panic("ydb: unknown variant type")
		}

	case *Ydb.Type_VoidType:
		return Void()

	default:
		panic("ydb: unknown type")
	}
}

func primitiveTypeFromYDB(t Ydb.Type_PrimitiveTypeId) T {
	switch t {
	case Ydb.Type_BOOL:
		return TypeBool
	case Ydb.Type_INT8:
		return TypeInt8
	case Ydb.Type_UINT8:
		return TypeUint8
	case Ydb.Type_INT16:
		return TypeInt16
	case Ydb.Type_UINT16:
		return TypeUint16
	case Ydb.Type_INT32:
		return TypeInt32
	case Ydb.Type_UINT32:
		return TypeUint32
	case Ydb.Type_INT64:
		return TypeInt64
	case Ydb.Type_UINT64:
		return TypeUint64
	case Ydb.Type_FLOAT:
		return TypeFloat
	case Ydb.Type_DOUBLE:
		return TypeDouble
	case Ydb.Type_DATE:
		return TypeDate
	case Ydb.Type_DATETIME:
		return TypeDatetime
	case Ydb.Type_TIMESTAMP:
		return TypeTimestamp
	case Ydb.Type_INTERVAL:
		return TypeInterval
	case Ydb.Type_TZ_DATE:
		return TypeTzDate
	case Ydb.Type_TZ_DATETIME:
		return TypeTzDatetime
	case Ydb.Type_TZ_TIMESTAMP:
		return TypeTzTimestamp
	case Ydb.Type_STRING:
		return TypeString
	case Ydb.Type_UTF8:
		return TypeUTF8
	case Ydb.Type_YSON:
		return TypeYSON
	case Ydb.Type_JSON:
		return TypeJSON
	case Ydb.Type_UUID:
		return TypeUUID
	case Ydb.Type_JSON_DOCUMENT:
		return TypeJSONDocument
	case Ydb.Type_DYNUMBER:
		return TypeDyNumber
	default:
		panic("ydb: unexpected type")
	}
}

func TypesFromYDB(es []*Ydb.Type) []T {
	ts := make([]T, len(es))
	for i, el := range es {
		ts[i] = TypeFromYDB(el)
	}
	return ts
}

func StructFields(ms []*Ydb.StructMember) []StructField {
	fs := make([]StructField, len(ms))
	for i, m := range ms {
		fs[i] = StructField{
			Name: m.Name,
			T:    TypeFromYDB(m.Type),
		}
	}
	return fs
}

func TypesEqual(a, b T) bool {
	return a.equalsTo(b)
}
