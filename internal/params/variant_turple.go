package params

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	variantTuple struct {
		parent *variant

		types []types.Type
		index uint32
		value value.Value
	}

	variantTupleTypes struct {
		tuple *variantTuple
	}

	variantTupleItem struct {
		tuple *variantTuple
	}

	variantTupleBuilder struct {
		tuple *variantTuple
	}
)

func (vt *variantTuple) Types() *variantTupleTypes {
	return &variantTupleTypes{
		tuple: vt,
	}
}

func (vtt *variantTupleTypes) AddTypes(args ...types.Type) *variantTupleTypes {
	vtt.tuple.types = append(vtt.tuple.types, args...)

	return vtt
}

// EACH TYPE

func (vtt *variantTupleTypes) Bool() *variantTupleTypes {
	vtt.tuple.types = append(vtt.tuple.types, types.Bool)

	return vtt
}

func (vtt *variantTupleTypes) Int64() *variantTupleTypes {
	vtt.tuple.types = append(vtt.tuple.types, types.Int64)

	return vtt
}

//

func (vtt *variantTupleTypes) Index(i uint32) *variantTupleItem {
	vtt.tuple.index = i

	return &variantTupleItem{
		tuple: vtt.tuple,
	}
}

// all types
func (vti *variantTupleItem) Bool(v bool) *variantTupleBuilder {
	vti.tuple.value = value.BoolValue(v)

	return &variantTupleBuilder{
		tuple: vti.tuple,
	}
}

func (vti *variantTupleItem) Text(v string) *variantTupleBuilder {
	vti.tuple.value = value.TextValue(v)

	return &variantTupleBuilder{
		tuple: vti.tuple,
	}
}

//

func (vtb *variantTupleBuilder) EndTuple() *variantBuilder {
	vtb.tuple.parent.value = value.VariantValueTuple(vtb.tuple.value, vtb.tuple.index, types.NewVariantTuple(vtb.tuple.types...))

	return &variantBuilder{
		variant: vtb.tuple.parent,
	}
}
