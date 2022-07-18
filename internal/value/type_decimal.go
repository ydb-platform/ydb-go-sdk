package value

import (
	"bytes"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type DecimalType struct {
	Precision uint32
	Scale     uint32
}

func (v *DecimalType) toString(buffer *bytes.Buffer) {
	buffer.WriteString(fmt.Sprintf("Decimal(%d,%d)", v.Precision, v.Scale))
}

func (v *DecimalType) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *DecimalType) equalsTo(rhs T) bool {
	vv, ok := rhs.(*DecimalType)
	return ok && *v == *vv
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
