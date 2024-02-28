package params

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	list struct {
		parent Builder
		name   string
		values []value.Value
	}
	listItem struct {
		parent *list
	}
)

func (l *list) AddItem() *listItem {
	return &listItem{
		parent: l,
	}
}

func (l *list) Build() Builder {
	l.parent.params = append(l.parent.params, &Parameter{
		parent: l.parent,
		name:   l.name,
		value:  value.ListValue(l.values...),
	})

	return l.parent
}

func (l *listItem) Text(v string) *list {
	l.parent.values = append(l.parent.values, value.TextValue(v))

	return l.parent
}

func (l *listItem) Bytes(v []byte) *list {
	l.parent.values = append(l.parent.values, value.BytesValue(v))

	return l.parent
}

func (l *listItem) Bool(v bool) *list {
	l.parent.values = append(l.parent.values, value.BoolValue(v))

	return l.parent
}

func (l *listItem) Uint64(v uint64) *list {
	l.parent.values = append(l.parent.values, value.Uint64Value(v))

	return l.parent
}

func (l *listItem) Int64(v int64) *list {
	l.parent.values = append(l.parent.values, value.Int64Value(v))

	return l.parent
}

func (l *listItem) Uint32(v uint32) *list {
	l.parent.values = append(l.parent.values, value.Uint32Value(v))

	return l.parent
}

func (l *listItem) Int32(v int32) *list {
	l.parent.values = append(l.parent.values, value.Int32Value(v))

	return l.parent
}

func (l *listItem) Uint16(v uint16) *list {
	l.parent.values = append(l.parent.values, value.Uint16Value(v))

	return l.parent
}

func (l *listItem) Int16(v int16) *list {
	l.parent.values = append(l.parent.values, value.Int16Value(v))

	return l.parent
}

func (l *listItem) Uint8(v uint8) *list {
	l.parent.values = append(l.parent.values, value.Uint8Value(v))

	return l.parent
}

func (l *listItem) Int8(v int8) *list {
	l.parent.values = append(l.parent.values, value.Int8Value(v))

	return l.parent
}

func (l *listItem) Float(v float32) *list {
	l.parent.values = append(l.parent.values, value.FloatValue(v))

	return l.parent
}

func (l *listItem) Double(v float64) *list {
	l.parent.values = append(l.parent.values, value.DoubleValue(v))

	return l.parent
}

func (l *listItem) Decimal(v [16]byte, precision, scale uint32) *list {
	l.parent.values = append(l.parent.values, value.DecimalValue(v, precision, scale))

	return l.parent
}

func (l *listItem) Timestamp(v time.Time) *list {
	l.parent.values = append(l.parent.values, value.TimestampValueFromTime(v))

	return l.parent
}

func (l *listItem) Date(v time.Time) *list {
	l.parent.values = append(l.parent.values, value.DateValueFromTime(v))

	return l.parent
}

func (l *listItem) Datetime(v time.Time) *list {
	l.parent.values = append(l.parent.values, value.DatetimeValueFromTime(v))

	return l.parent
}

func (l *listItem) Interval(v time.Duration) *list {
	l.parent.values = append(l.parent.values, value.IntervalValueFromDuration(v))

	return l.parent
}
