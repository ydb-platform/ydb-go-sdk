package params

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	dict struct {
		parent Builder
		name   string
		values []value.DictValueField
	}
	dictPair struct {
		parent   *dict
		keyValue value.Value
	}
	dictValue struct {
		pair *dictPair
	}
)

func (d *dict) Pair() *dictPair {
	return &dictPair{
		parent: d,
	}
}

func (d *dict) AddPairs(pairs ...value.DictValueField) *dict {
	d.values = append(d.values, pairs...)

	return d
}

func (d *dictPair) Text(v string) *dictValue {
	d.keyValue = value.TextValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Bytes(v []byte) *dictValue {
	d.keyValue = value.BytesValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Bool(v bool) *dictValue {
	d.keyValue = value.BoolValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Uint64(v uint64) *dictValue {
	d.keyValue = value.Uint64Value(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Int64(v int64) *dictValue {
	d.keyValue = value.Int64Value(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Uint32(v uint32) *dictValue {
	d.keyValue = value.Uint32Value(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Int32(v int32) *dictValue {
	d.keyValue = value.Int32Value(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Uint16(v uint16) *dictValue {
	d.keyValue = value.Uint16Value(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Int16(v int16) *dictValue {
	d.keyValue = value.Int16Value(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Uint8(v uint8) *dictValue {
	d.keyValue = value.Uint8Value(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Int8(v int8) *dictValue {
	d.keyValue = value.Int8Value(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Float(v float32) *dictValue {
	d.keyValue = value.FloatValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Double(v float64) *dictValue {
	d.keyValue = value.DoubleValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Decimal(v [16]byte, precision, scale uint32) *dictValue {
	d.keyValue = value.DecimalValue(v, precision, scale)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Timestamp(v time.Time) *dictValue {
	d.keyValue = value.TimestampValueFromTime(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Date(v time.Time) *dictValue {
	d.keyValue = value.DateValueFromTime(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Datetime(v time.Time) *dictValue {
	d.keyValue = value.DatetimeValueFromTime(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) Interval(v time.Duration) *dictValue {
	d.keyValue = value.IntervalValueFromDuration(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) JSON(v string) *dictValue {
	d.keyValue = value.JSONValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) JSONDocument(v string) *dictValue {
	d.keyValue = value.JSONDocumentValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) YSON(v []byte) *dictValue {
	d.keyValue = value.YSONValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictPair) UUID(v [16]byte) *dictValue {
	d.keyValue = value.UUIDValue(v)

	return &dictValue{
		pair: d,
	}
}

func (d *dictValue) Uint64(v uint64) *dict {
	d.pair.parent.values = append(d.pair.parent.values, value.DictValueField{
		K: d.pair.keyValue,
		V: value.Uint64Value(v),
	})

	return d.pair.parent
}

func (d *dict) Build() Builder {
	d.parent.params = append(d.parent.params, &Parameter{
		parent: d.parent,
		name:   d.name,
		value:  value.DictValue(d.values...),
	})

	return d.parent
}
