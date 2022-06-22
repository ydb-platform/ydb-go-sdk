package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type tzTimestampValue struct {
	v string
}

func (v *tzTimestampValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v *tzTimestampValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (*tzTimestampValue) getType() T {
	return TypeTzTimestamp
}

func (*tzTimestampValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeTzTimestamp]
}

func (v *tzTimestampValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Text()
	if v != nil {
		vv.TextValue = v.v
	}

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TzTimestampValue(v string) *tzTimestampValue {
	return &tzTimestampValue{v: v}
}
