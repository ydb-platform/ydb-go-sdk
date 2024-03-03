package params

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	set struct {
		parent Builder
		name   string
		values map[value.Value]struct{}
	}

	setItem struct {
		parent *set
	}
)

func (s *set) AddItem() *setItem {
	return &setItem{
		parent: s,
	}
}

func (s *set) Build() Builder {
	values := make([]value.Value, 0, len(s.values))

	for el := range s.values {
		values = append(values, el)
	}

	s.parent.params = append(s.parent.params, &Parameter{
		parent: s.parent,
		name:   s.name,
		value:  value.SetValue(values...),
	})

	return s.parent
}

func (s *setItem) Text(v string) *set {
	s.parent.values[value.TextValue(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Bytes(v []byte) *set {
	s.parent.values[value.BytesValue(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Bool(v bool) *set {
	s.parent.values[value.BoolValue(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Uint64(v uint64) *set {
	s.parent.values[value.Uint64Value(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Int64(v int64) *set {
	s.parent.values[value.Int64Value(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Uint32(v uint32) *set {
	s.parent.values[value.Uint32Value(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Int32(v int32) *set {
	s.parent.values[value.Int32Value(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Uint16(v uint16) *set {
	s.parent.values[value.Uint16Value(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Int16(v int16) *set {
	s.parent.values[value.Int16Value(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Uint8(v uint8) *set {
	s.parent.values[value.Uint8Value(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Int8(v int8) *set {
	s.parent.values[value.Int8Value(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Float(v float32) *set {
	s.parent.values[value.FloatValue(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Double(v float64) *set {
	s.parent.values[value.DoubleValue(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Decimal(v [16]byte, precision, scale uint32) *set {
	s.parent.values[value.DecimalValue(v, precision, scale)] = struct{}{}

	return s.parent
}

func (s *setItem) Timestamp(v time.Time) *set {
	s.parent.values[value.TimestampValueFromTime(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Date(v time.Time) *set {
	s.parent.values[value.DateValueFromTime(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Datetime(v time.Time) *set {
	s.parent.values[value.DatetimeValueFromTime(v)] = struct{}{}

	return s.parent
}

func (s *setItem) Interval(v time.Duration) *set {
	s.parent.values[value.IntervalValueFromDuration(v)] = struct{}{}

	return s.parent
}
