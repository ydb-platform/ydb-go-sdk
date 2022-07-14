package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

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
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
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
