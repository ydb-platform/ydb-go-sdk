package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type intervalValue int64

func (v intervalValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v intervalValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (intervalValue) getType() T {
	return TypeInterval
}

func (intervalValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeInterval]
}

func (v intervalValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Int64()
	vv.Int64Value = int64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

// IntervalValue makes Value from given microseconds value
func IntervalValue(v int64) intervalValue {
	return intervalValue(v)
}
