package params

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type (
	Builder struct {
		params Parameters
	}
	Parameters []*Parameter
)

func (p *Parameters) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteByte('{')
	if p != nil {
		for i, param := range *p {
			if i != 0 {
				buffer.WriteByte(',')
			}
			buffer.WriteByte('"')
			buffer.WriteString(param.name)
			buffer.WriteString("\":")
			buffer.WriteString(param.value.Yql())
		}
	}
	buffer.WriteByte('}')

	return buffer.String()
}

func (p *Parameters) ToYDB(a *allocator.Allocator) map[string]*Ydb.TypedValue {
	parameters := make(map[string]*Ydb.TypedValue, len(*p))
	for _, param := range *p {
		parameters[param.name] = value.ToYDB(param.value, a)
	}

	return parameters
}

func (p *Parameters) Each(it func(name string, v value.Value)) {
	if p == nil {
		return
	}
	for _, p := range *p {
		it(p.name, p.value)
	}
}

func (p *Parameters) Count() int {
	if p == nil {
		return 0
	}

	return len(*p)
}

func (p *Parameters) Add(params ...*Parameter) {
	for _, param := range params {
		*p = append(*p, param)
	}
}

func Nil() *Parameters {
	return &Parameters{}
}

func (b Builder) Build() *Parameters {
	return &b.params
}

func (b Builder) Param(name string) *Parameter {
	return &Parameter{
		parent: b,
		name:   name,
		value:  nil,
	}
}

func (p *Parameter) Text(v string) Builder {
	p.value = value.TextValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Bytes(v []byte) Builder {
	p.value = value.BytesValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Bool(v bool) Builder {
	p.value = value.BoolValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Uint64(v uint64) Builder {
	p.value = value.Uint64Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Int64(v int64) Builder {
	p.value = value.Int64Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Uint32(v uint32) Builder {
	p.value = value.Uint32Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Int32(v int32) Builder {
	p.value = value.Int32Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Uint16(v uint16) Builder {
	p.value = value.Uint16Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Int16(v int16) Builder {
	p.value = value.Int16Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Uint8(v uint8) Builder {
	p.value = value.Uint8Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Int8(v int8) Builder {
	p.value = value.Int8Value(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Float(v float32) Builder {
	p.value = value.FloatValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Double(v float64) Builder {
	p.value = value.DoubleValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Decimal(v [16]byte, precision, scale uint32) Builder {
	p.value = value.DecimalValue(v, precision, scale)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Timestamp(v time.Time) Builder {
	p.value = value.TimestampValueFromTime(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Date(v time.Time) Builder {
	p.value = value.DateValueFromTime(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Datetime(v time.Time) Builder {
	p.value = value.DatetimeValueFromTime(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Interval(v time.Duration) Builder {
	p.value = value.IntervalValueFromDuration(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}
