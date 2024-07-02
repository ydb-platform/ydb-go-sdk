package params

import (
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type (
	NamedValue interface {
		Name() string
		Value() value.Value
	}
	Parameter struct {
		parent Builder
		name   string
		value  value.Value
	}
	Parameters []*Parameter
)

func Named(name string, value value.Value) *Parameter {
	return &Parameter{
		name:  name,
		value: value,
	}
}

func (p *Parameter) Name() string {
	return p.name
}

func (p *Parameter) Value() value.Value {
	return p.value
}

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
	if p == nil {
		return nil
	}
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

func (p *Parameters) Add(params ...NamedValue) {
	for _, param := range params {
		*p = append(*p, Named(param.Name(), param.Value()))
	}
}

func (p *Parameter) BeginOptional() *optional {
	return &optional{
		parent: p.parent,
		name:   p.name,
	}
}

func (p *Parameter) BeginList() *list {
	return &list{
		parent: p.parent,
		name:   p.name,
	}
}

func (p *Parameter) Pg() pgParam {
	return pgParam{p}
}

func (p *Parameter) BeginSet() *set {
	return &set{
		parent: p.parent,
		name:   p.name,
	}
}

func (p *Parameter) BeginDict() *dict {
	return &dict{
		parent: p.parent,
		name:   p.name,
	}
}

func (p *Parameter) BeginTuple() *tuple {
	return &tuple{
		parent: p.parent,
		name:   p.name,
	}
}

func (p *Parameter) BeginStruct() *structure {
	return &structure{
		parent: p.parent,
		name:   p.name,
	}
}

func (p *Parameter) BeginVariant() *variant {
	return &variant{
		parent: p.parent,
		name:   p.name,
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

func (p *Parameter) JSON(v string) Builder {
	p.value = value.JSONValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) JSONDocument(v string) Builder {
	p.value = value.JSONDocumentValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) YSON(v []byte) Builder {
	p.value = value.YSONValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) UUID(v [16]byte) Builder {
	p.value = value.UUIDValue(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) Any(v types.Value) Builder {
	p.value = v
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) TzDate(v time.Time) Builder {
	p.value = value.TzDateValueFromTime(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) TzTimestamp(v time.Time) Builder {
	p.value = value.TzTimestampValueFromTime(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func (p *Parameter) TzDatetime(v time.Time) Builder {
	p.value = value.TzDatetimeValueFromTime(v)
	p.parent.params = append(p.parent.params, p)

	return p.parent
}

func Declare(p *Parameter) string {
	return fmt.Sprintf(
		"DECLARE %s AS %s",
		p.name,
		p.value.Type().Yql(),
	)
}
