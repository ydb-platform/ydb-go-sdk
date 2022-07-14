package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type tzDateValue struct {
	v string
}

func (v *tzDateValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *tzDateValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (*tzDateValue) Type() T {
	return TypeTzDate
}

func (v *tzDateValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzDateValue(v string) *tzDateValue {
	return &tzDateValue{v: v}
}
