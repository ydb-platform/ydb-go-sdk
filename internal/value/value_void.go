package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type voidValue struct{}

func (v voidValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v voidValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

var (
	_voidValueType = voidType{}
	_voidValue     = &Ydb.Value{
		Value: new(Ydb.Value_NullFlagValue),
	}
)

func (voidValue) Type() T {
	return _voidValueType
}

func (voidValue) toYDB(*allocator.Allocator) *Ydb.Value {
	return _voidValue
}

func VoidValue() voidValue {
	return voidValue{}
}
