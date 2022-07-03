package value

import (
	"bytes"
	"encoding/binary"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type decimalValue struct {
	v [16]byte
	t *DecimalType
}

func (v decimalValue) toString(buffer *bytes.Buffer) {
	a := allocator.New()
	defer a.Free()
	v.Type().toString(buffer)
	valueToString(buffer, v.Type(), v.toYDB(a))
}

func (v decimalValue) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v decimalValue) Type() T {
	return v.t
}

func (v *decimalValue) toYDB(a *allocator.Allocator) *Ydb.Value {
	var bytes [16]byte
	if v != nil {
		bytes = v.v
	}
	vv := a.Low128()
	vv.Low_128 = binary.BigEndian.Uint64(bytes[8:16])

	vvv := a.Value()
	vvv.High_128 = binary.BigEndian.Uint64(bytes[0:8])
	vvv.Value = vv

	return vvv
}

func DecimalValue(v [16]byte, precision uint32, scale uint32) *decimalValue {
	return &decimalValue{
		v: v,
		t: &DecimalType{
			Precision: precision,
			Scale:     scale,
		},
	}
}
