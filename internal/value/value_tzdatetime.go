package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type tzDatetimeValue struct {
	v string
}

func (v *tzDatetimeValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *tzDatetimeValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*tzDatetimeValue) Type() T {
	return TypeTzDatetime
}

func (v *tzDatetimeValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzDatetimeValue(v string) *tzDatetimeValue {
	return &tzDatetimeValue{v: v}
}
