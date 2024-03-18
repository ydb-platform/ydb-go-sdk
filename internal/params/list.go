package params

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	listParent interface {
		append(name string, value value.Value)
	}
	list[T listParent] struct {
		parent T
		name   string
		values []value.Value
	}
	listItemParent interface {
		appendItem(value value.Value)
	}
	listItem[T listItemParent] struct {
		parent T
	}
)

func (l *list[T]) appendItem(value value.Value) {
	l.values = append(l.values, value)
}

func (l *list[T]) append(name string, value value.Value) {
	l.parent.append(name, value)
}

func (l *list[T]) Add() *listItem[*list[T]] {
	return &listItem[*list[T]]{
		parent: l,
	}
}

func (l *list[T]) AddItems(items ...value.Value) *list[T] {
	l.values = append(l.values, items...)

	return l
}

func (l *list[T]) BeginList() *list[*list[T]] {
	return &list[*list[T]]{
		parent: l,
	}
}

func (l *list[T]) EndList() T {
	l.parent.append(l.name, value.ListValue(l.values...))

	return l.parent
}

func (l listItem[T]) Text(v string) T {
	l.parent.appendItem(value.TextValue(v))

	return l.parent
}

func (l listItem[T]) Bytes(v []byte) T {
	l.parent.appendItem(value.BytesValue(v))

	return l.parent
}

func (l listItem[T]) Bool(v bool) T {
	l.parent.appendItem(value.BoolValue(v))

	return l.parent
}

func (l listItem[T]) Uint64(v uint64) T {
	l.parent.appendItem(value.Uint64Value(v))

	return l.parent
}

func (l listItem[T]) Int64(v int64) T {
	l.parent.appendItem(value.Int64Value(v))

	return l.parent
}

func (l listItem[T]) Uint32(v uint32) T {
	l.parent.appendItem(value.Uint32Value(v))

	return l.parent
}

func (l listItem[T]) Int32(v int32) T {
	l.parent.appendItem(value.Int32Value(v))

	return l.parent
}

func (l listItem[T]) Uint16(v uint16) T {
	l.parent.appendItem(value.Uint16Value(v))

	return l.parent
}

func (l listItem[T]) Int16(v int16) T {
	l.parent.appendItem(value.Int16Value(v))

	return l.parent
}

func (l listItem[T]) Uint8(v uint8) T {
	l.parent.appendItem(value.Uint8Value(v))

	return l.parent
}

func (l listItem[T]) Int8(v int8) T {
	l.parent.appendItem(value.Int8Value(v))

	return l.parent
}

func (l listItem[T]) Float(v float32) T {
	l.parent.appendItem(value.FloatValue(v))

	return l.parent
}

func (l listItem[T]) Double(v float64) T {
	l.parent.appendItem(value.DoubleValue(v))

	return l.parent
}

func (l listItem[T]) Decimal(v [16]byte, precision, scale uint32) T {
	l.parent.appendItem(value.DecimalValue(v, precision, scale))

	return l.parent
}

func (l listItem[T]) Timestamp(v time.Time) T {
	l.parent.appendItem(value.TimestampValueFromTime(v))

	return l.parent
}

func (l listItem[T]) Date(v time.Time) T {
	l.parent.appendItem(value.DateValueFromTime(v))

	return l.parent
}

func (l listItem[T]) Datetime(v time.Time) T {
	l.parent.appendItem(value.DatetimeValueFromTime(v))

	return l.parent
}

func (l listItem[T]) Interval(v time.Duration) T {
	l.parent.appendItem(value.IntervalValueFromDuration(v))

	return l.parent
}

func (l listItem[T]) JSON(v string) T {
	l.parent.appendItem(value.JSONValue(v))

	return l.parent
}

func (l listItem[T]) JSONDocument(v string) T {
	l.parent.appendItem(value.JSONDocumentValue(v))

	return l.parent
}

func (l listItem[T]) YSON(v []byte) T {
	l.parent.appendItem(value.YSONValue(v))

	return l.parent
}

func (l listItem[T]) UUID(v [16]byte) T {
	l.parent.appendItem(value.UUIDValue(v))

	return l.parent
}
