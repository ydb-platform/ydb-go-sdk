package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type timestampValue uint64

func (v timestampValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v timestampValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (timestampValue) getType() T {
	return TypeTimestamp
}

func (timestampValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeTimestamp]
}

func (v timestampValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint64()
	vv.Uint64Value = uint64(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func TimestampValue(v uint64) timestampValue {
	return timestampValue(v)
}
