package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type voidValue struct{}

func (v voidValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.getType().toString(buffer)
	valueToString(buffer, v.getType(), v.toYDBValue(a))
}

func (v voidValue) String() string {
	var buf bytes.Buffer
	v.toString(&buf)
	return buf.String()
}

var (
	_voidValueType = voidType{}
	_voidValue     = &Ydb.Value{
		Value: new(Ydb.Value_NullFlagValue),
	}
)

func (voidValue) getType() T {
	return _voidValueType
}

func (voidValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	return _voidValueType.toYDB(a)
}

func (voidValue) toYDBValue(*allocator.Allocator) *Ydb.Value {
	return _voidValue
}

func VoidValue() voidValue {
	return voidValue{}
}
