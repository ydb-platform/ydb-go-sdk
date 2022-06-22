package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type tzDateValue struct {
	v string
}

func (v *tzDateValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *tzDateValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*tzDateValue) getType() T {
	return TypeTzDate
}

func (*tzDateValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeTzDate]
}

func (v *tzDateValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
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
