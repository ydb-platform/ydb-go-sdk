package value

import (
	"bytes"
	"strconv"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type DecimalType struct {
	Precision uint32
	Scale     uint32
}

func (v *DecimalType) toString(buffer *bytes.Buffer) {
	buffer.WriteString("Decimal(")
	buffer.WriteString(strconv.FormatUint(uint64(v.Precision), 10))
	buffer.WriteByte(',')
	buffer.WriteString(strconv.FormatUint(uint64(v.Scale), 10))
	buffer.WriteString(")")
}

func (v *DecimalType) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *DecimalType) equalsTo(rhs T) bool {
	vv, ok := rhs.(*DecimalType)
	if !ok {
		return false
	}
	if v.Precision != vv.Precision {
		return false
	}
	if v.Scale != vv.Scale {
		return false
	}
	return true
}

func (v *DecimalType) toYDB(a *allocator.Allocator) *Ydb.Type {
	decimal := a.Decimal()

	decimal.Scale = v.Scale
	decimal.Precision = v.Precision

	typeDecimal := a.TypeDecimal()
	typeDecimal.DecimalType = decimal

	t := a.Type()
	t.Type = typeDecimal

	return t
}

func Decimal(precision, scale uint32) *DecimalType {
	return &DecimalType{
		Precision: precision,
		Scale:     scale,
	}
}
