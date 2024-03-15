package params

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	optional struct {
		parent Builder
		name   string
		value  value.Value
	}
	optionalBuilder struct {
		opt *optional
	}
)

func (b *optionalBuilder) EndOptional() Builder {
	b.opt.parent.params = append(b.opt.parent.params, &Parameter{
		parent: b.opt.parent,
		name:   b.opt.name,
		value:  value.OptionalValue(b.opt.value),
	})

	return b.opt.parent
}

func (p *optional) Text(v string) *optionalBuilder {
	p.value = value.TextValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Bytes(v []byte) *optionalBuilder {
	p.value = value.BytesValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Bool(v bool) *optionalBuilder {
	p.value = value.BoolValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Uint64(v uint64) *optionalBuilder {
	p.value = value.Uint64Value(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Int64(v int64) *optionalBuilder {
	p.value = value.Int64Value(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Uint32(v uint32) *optionalBuilder {
	p.value = value.Uint32Value(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Int32(v int32) *optionalBuilder {
	p.value = value.Int32Value(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Uint16(v uint16) *optionalBuilder {
	p.value = value.Uint16Value(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Int16(v int16) *optionalBuilder {
	p.value = value.Int16Value(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Uint8(v uint8) *optionalBuilder {
	p.value = value.Uint8Value(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Int8(v int8) *optionalBuilder {
	p.value = value.Int8Value(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Float(v float32) *optionalBuilder {
	p.value = value.FloatValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Double(v float64) *optionalBuilder {
	p.value = value.DoubleValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Decimal(v [16]byte, precision, scale uint32) *optionalBuilder {
	p.value = value.DecimalValue(v, precision, scale)

	return &optionalBuilder{opt: p}
}

func (p *optional) Timestamp(v time.Time) *optionalBuilder {
	p.value = value.TimestampValueFromTime(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Date(v time.Time) *optionalBuilder {
	p.value = value.DateValueFromTime(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Datetime(v time.Time) *optionalBuilder {
	p.value = value.DatetimeValueFromTime(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) Interval(v time.Duration) *optionalBuilder {
	p.value = value.IntervalValueFromDuration(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) JSON(v string) *optionalBuilder {
	p.value = value.JSONValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) JSONDocument(v string) *optionalBuilder {
	p.value = value.JSONDocumentValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) YSON(v []byte) *optionalBuilder {
	p.value = value.YSONValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) UUID(v [16]byte) *optionalBuilder {
	p.value = value.UUIDValue(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) TzDate(v time.Time) *optionalBuilder {
	p.value = value.TzDateValueFromTime(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) TzTimestamp(v time.Time) *optionalBuilder {
	p.value = value.TzTimestampValueFromTime(v)

	return &optionalBuilder{opt: p}
}

func (p *optional) TzDatetime(v time.Time) *optionalBuilder {
	p.value = value.TzDatetimeValueFromTime(v)

	return &optionalBuilder{opt: p}
}
