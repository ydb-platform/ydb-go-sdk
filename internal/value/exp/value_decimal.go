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

func (v *decimalValue) toYDBType(a *allocator.Allocator) *Ydb.Type {
	var precision, scale uint32
	if v != nil {
		precision = v.precision
		scale = v.scale
	}
	decimal := a.Decimal()

	decimal.Scale = scale
	decimal.Precision = precision

	typeDecimal := a.TypeDecimal()
	typeDecimal.DecimalType = decimal

	t := a.Type()
	t.Type = typeDecimal

	return t
}

func (v *decimalValue) toYDBValue(a *allocator.Allocator) *Ydb.Value {
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
		v:         v,
		precision: precision,
		scale:     scale,
	}
}
