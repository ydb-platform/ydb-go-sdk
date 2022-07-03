package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type nullValue struct {
	t T
}

func (v *nullValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v *nullValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *nullValue) Type() T {
	return v.t
}

func (v *nullValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	vv := a.Value()
	vv.Value = a.NullFlag()

	x := v.t
	for {
		opt, ok := x.(*optionalType)
		if !ok {
			break
		}
		x = opt.t
		nestedValue := a.Nested()
		nestedValue.NestedValue = vv
		vv = a.Value()
		vv.Value = nestedValue
	}

	return vv
}

func NullValue(t T) *nullValue {
	return &nullValue{
		t: Optional(t),
	}
}
