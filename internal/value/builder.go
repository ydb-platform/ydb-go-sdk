package value

type (
	Builder     struct{}
	builderList struct {
		items []Value
	}
	builderTypedList[T any] struct {
		parent *builderList
		add    func(T) Value
	}
)

func (b Builder) List() *builderList {
	return &builderList{}
}

func (b *builderList) Uint8() *builderTypedList[uint8] {
	return &builderTypedList[uint8]{
		parent: b,
		add: func(v uint8) Value {
			return Uint8Value(v)
		},
	}
}

func (b *builderList) Uint16() *builderTypedList[uint16] {
	return &builderTypedList[uint16]{
		parent: b,
		add: func(v uint16) Value {
			return Uint16Value(v)
		},
	}
}

func (b *builderList) Uint32() *builderTypedList[uint32] {
	return &builderTypedList[uint32]{
		parent: b,
		add: func(v uint32) Value {
			return Uint32Value(v)
		},
	}
}

func (b *builderList) Uint64() *builderTypedList[uint64] {
	return &builderTypedList[uint64]{
		parent: b,
		add: func(v uint64) Value {
			return Uint64Value(v)
		},
	}
}

func (b *builderList) Int8() *builderTypedList[int8] {
	return &builderTypedList[int8]{
		parent: b,
		add: func(v int8) Value {
			return Int8Value(v)
		},
	}
}

func (b *builderList) Int16() *builderTypedList[int16] {
	return &builderTypedList[int16]{
		parent: b,
		add: func(v int16) Value {
			return Int16Value(v)
		},
	}
}

func (b *builderList) Int32() *builderTypedList[int32] {
	return &builderTypedList[int32]{
		parent: b,
		add: func(v int32) Value {
			return Int32Value(v)
		},
	}
}

func (b *builderList) Int64() *builderTypedList[int64] {
	return &builderTypedList[int64]{
		parent: b,
		add: func(v int64) Value {
			return Int64Value(v)
		},
	}
}

func (b *builderTypedList[T]) Add(v T) *builderTypedList[T] {
	b.parent.items = append(b.parent.items, b.add(v))

	return b
}

func (b *builderTypedList[T]) Build() Value {
	return ListValue(b.parent.items...)
}
