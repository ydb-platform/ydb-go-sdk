package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type datetimeValue uint32

func (v datetimeValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v datetimeValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

func (datetimeValue) getType() T {
	return TypeDatetime
}

func (datetimeValue) toYDBType(*allocator.Allocator) *Ydb.Type {
	return primitive[TypeDatetime]
}

func (v datetimeValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Uint32()
	vv.Uint32Value = uint32(v)

	vvv := a.Value()
	vvv.Value = vv

	return vvv
}

func DatetimeValue(v uint32) datetimeValue {
	return datetimeValue(v)
}
