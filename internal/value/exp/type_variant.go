package value

import (
	"bytes"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type internalVariantType uint8

const (
	variantTypeUndefined internalVariantType = iota
	variantTypeTuple
	variantTypeStruct
)

type variantType struct {
	t  T
	tt internalVariantType
}

func (v *variantType) toString(buffer *bytes.Buffer) {
	buffer.WriteString("Variant<")
	v.t.toString(buffer)
	buffer.WriteString(">")
}

func (v *variantType) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (v *variantType) equalsTo(rhs T) bool {
	vv, ok := rhs.(*variantType)
	if !ok {
		return false
	}
	return v.t.equalsTo(vv.t)
}

func (v *variantType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeVariant := a.TypeVariant()

	typeVariant.VariantType = a.Variant()

	tt := v.t.toYDB(a).Type

	switch v.tt {
	case variantTypeTuple:
		tupleType, ok := tt.(*Ydb.Type_TupleType)
		if !ok {
			panic(fmt.Sprintf("type %T cannot casts to *Ydb.Type_TupleType", tt))
		}

		tupleItems := a.VariantTupleItems()
		tupleItems.TupleItems = tupleType.TupleType

		typeVariant.VariantType.Type = tupleItems
	case variantTypeStruct:
		structType, ok := tt.(*Ydb.Type_StructType)
		if !ok {
			panic(fmt.Sprintf("type %T cannot casts to *Ydb.Type_TupleType", tt))
		}

		structItems := a.VariantStructItems()
		structItems.StructItems = structType.StructType

		typeVariant.VariantType.Type = structItems
	default:
		panic(fmt.Sprintf("unsupported variant type: %v", v.tt))
	}

	t.Type = typeVariant

	return t
}

func Variant(t T) *variantType {
	if tt, ok := t.(*variantType); ok {
		t = tt.t
	}
	var tt internalVariantType
	switch t.(type) {
	case *StructType:
		tt = variantTypeStruct
	case *TupleType:
		tt = variantTypeTuple
	default:
		panic(fmt.Sprintf("unsupported variant type: %v", t))
	}
	return &variantType{
		t:  t,
		tt: tt,
	}
}
