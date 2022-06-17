// go:build +go1.18

package allocator

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"sync"
)

type (
	putter interface {
		Put()
	}
	free struct {
		v any
		p *sync.Pool
	}
	Allocator struct {
		allocations []free
	}
	pool[T any] struct {
		sync.Pool
	}
)

func (p *pool[T]) Get() (v *T) {
	defer func() {
		if vv, ok := (any)(v).(interface {
			Reset()
		}); ok {
			vv.Reset()
		}
	}()
	return p.Pool.Get().(*T)
}

func (p *pool[T]) Put(t *T) {
	p.Pool.Put(t)
}

func makePool[T any]() *pool[T] {
	return &pool[T]{
		sync.Pool{
			New: func() any {
				return new(T)
			},
		},
	}
}

var (
	allocatorsPool    = makePool[Allocator]()
	valuePool         = makePool[Ydb.Value]()
	typePool          = makePool[Ydb.Type]()
	typePrimitivePool = makePool[Ydb.Type_TypeId]()
	typeDecimalPool   = makePool[Ydb.Type_DecimalType]()
	typeListPool      = makePool[Ydb.Type_ListType]()
	typeEmptyListPool = makePool[Ydb.Type_EmptyListType]()
	typeTuplePool     = makePool[Ydb.Type_TupleType]()
	decimalPool       = makePool[Ydb.DecimalType]()
	listPool          = makePool[Ydb.ListType]()
	tuplePool         = makePool[Ydb.TupleType]()
	typeOptionalPool  = makePool[Ydb.Type_OptionalType]()
	typedValuePool    = makePool[Ydb.TypedValue]()
	boolPool          = makePool[Ydb.Value_BoolValue]()
	bytesPool         = makePool[Ydb.Value_BytesValue]()
	textPool          = makePool[Ydb.Value_TextValue]()
	int32Pool         = makePool[Ydb.Value_Int32Value]()
	uint32Pool        = makePool[Ydb.Value_Uint32Value]()
	low128Pool        = makePool[Ydb.Value_Low_128]()
	int64Pool         = makePool[Ydb.Value_Int64Value]()
	uint64Pool        = makePool[Ydb.Value_Uint64Value]()
	floatPool         = makePool[Ydb.Value_FloatValue]()
	doublePool        = makePool[Ydb.Value_DoubleValue]()
)

func New() *Allocator {
	return allocatorsPool.Get()
}

func (a *Allocator) Close() {
	var l int
	for {
		l = len(a.allocations)
		if l == 0 {
			break
		}
		a.allocations[l-1].p.Put(a.allocations[l-1].v)
		a.allocations = a.allocations[:l-1]
	}
	allocatorsPool.Put(a)
}

func (a *Allocator) Value() (v *Ydb.Value) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &valuePool.Pool})
	}()
	return valuePool.Get()
}

func (a *Allocator) TypedValue() (v *Ydb.TypedValue) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &typedValuePool.Pool})
	}()
	return typedValuePool.Get()
}

func (a *Allocator) Type() (v *Ydb.Type) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &typePool.Pool})
	}()
	return typePool.Get()
}

func (a *Allocator) TypePrimitive() (v *Ydb.Type_TypeId) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &typePrimitivePool.Pool})
	}()
	return typePrimitivePool.Get()
}

func (a *Allocator) Decimal() (v *Ydb.DecimalType) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &decimalPool.Pool})
	}()
	return decimalPool.Get()
}

func (a *Allocator) List() (v *Ydb.ListType) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &listPool.Pool})
	}()
	return listPool.Get()
}

func (a *Allocator) Tuple() (v *Ydb.TupleType) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &tuplePool.Pool})
	}()
	return tuplePool.Get()
}

func (a *Allocator) TypeDecimal() (v *Ydb.Type_DecimalType) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &typeDecimalPool.Pool})
	}()
	return typeDecimalPool.Get()
}

func (a *Allocator) TypeList() (v *Ydb.Type_ListType) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &typeListPool.Pool})
	}()
	return typeListPool.Get()
}

func (a *Allocator) TypeTuple() (v *Ydb.Type_TupleType) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &typeTuplePool.Pool})
	}()
	return typeTuplePool.Get()
}

func (a *Allocator) EmptyTypeList() (v *Ydb.Type_EmptyListType) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &typeEmptyListPool.Pool})
	}()
	return typeEmptyListPool.Get()
}

func (a *Allocator) TypeOptional() (v *Ydb.Type_OptionalType) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &typeOptionalPool.Pool})
	}()
	return typeOptionalPool.Get()
}

func (a *Allocator) BoolValue() (v *Ydb.Value_BoolValue) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &boolPool.Pool})
	}()
	return boolPool.Get()
}

func (a *Allocator) BytesValue() (v *Ydb.Value_BytesValue) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &bytesPool.Pool})
	}()
	return bytesPool.Get()
}

func (a *Allocator) Int32Value() (v *Ydb.Value_Int32Value) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &int32Pool.Pool})
	}()
	return int32Pool.Get()
}

func (a *Allocator) Int64Value() (v *Ydb.Value_Int64Value) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &int64Pool.Pool})
	}()
	return int64Pool.Get()
}

func (a *Allocator) Uint32Value() (v *Ydb.Value_Uint32Value) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &uint32Pool.Pool})
	}()
	return uint32Pool.Get()
}

func (a *Allocator) FloatValue() (v *Ydb.Value_FloatValue) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &floatPool.Pool})
	}()
	return floatPool.Get()
}

func (a *Allocator) DoubleValue() (v *Ydb.Value_DoubleValue) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &doublePool.Pool})
	}()
	return doublePool.Get()
}

func (a *Allocator) Uint64Value() (v *Ydb.Value_Uint64Value) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &uint64Pool.Pool})
	}()
	return uint64Pool.Get()
}

func (a *Allocator) TextValue() (v *Ydb.Value_TextValue) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &textPool.Pool})
	}()
	return textPool.Get()
}

func (a *Allocator) Low128Value() (v *Ydb.Value_Low_128) {
	defer func() {
		a.allocations = append(a.allocations, free{v, &low128Pool.Pool})
	}()
	return low128Pool.Get()
}
