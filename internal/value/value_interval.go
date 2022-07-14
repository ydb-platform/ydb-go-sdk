package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type intervalValue int64

func (v intervalValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v intervalValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (intervalValue) Type() T {
	return TypeInterval
}

func (v intervalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
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
