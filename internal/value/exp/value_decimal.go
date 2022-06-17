package value

import (
	"encoding/binary"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type decimalValue struct {
	v         [16]byte
	precision uint32
	scale     uint32
}

func (v decimalValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	decimal := a.Decimal()

	decimal.Scale = v.scale
	decimal.Precision = v.precision

	typeId := a.TypeDecimal()
	typeId.DecimalType = decimal

	t := a.Type()
	t.Type = typeId

	return t
}

func (v decimalValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
	vv := a.Low128Value()
	vv.Low_128 = binary.BigEndian.Uint64(v.v[8:16])

	vvv := a.Value()
	vvv.High_128 = binary.BigEndian.Uint64(v.v[0:8])
	vvv.Value = vv

	return vvv
}

func DecimalValue(v [16]byte, precision uint32, scale uint32) decimalValue {
	return decimalValue{
		v:         v,
		precision: precision,
		scale:     scale,
	}
}
