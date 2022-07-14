package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
)

type (
	StructField struct {
		Name string
		T    T
	}
	StructType struct {
		fields []StructField
	}
)

func (v *StructType) toString(buffer *bytes.Buffer) {
	buffer.WriteString("Struct<")
	for i, f := range v.fields {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(f.Name)
		buffer.WriteByte(':')
		f.T.toString(buffer)
	}
	buffer.WriteByte('>')
}

func (v *StructType) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

func (v *StructType) equalsTo(rhs T) bool {
	vv, ok := rhs.(*StructType)
	if !ok {
		return false
	}
	if len(v.fields) != len(vv.fields) {
		return false
	}
	for i := range v.fields {
		if v.fields[i].Name != vv.fields[i].Name {
			return false
		}
		if !v.fields[i].T.equalsTo(vv.fields[i].T) {
			return false
		}
	}
	return true
}

func (v *StructType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeStruct := a.TypeStruct()

	typeStruct.StructType = a.Struct()

	for _, filed := range v.fields {
		structMember := a.StructMember()
		structMember.Name = filed.Name
		structMember.Type = filed.T.toYDB(a)
		typeStruct.StructType.Members = append(
			typeStruct.StructType.Members,
			structMember,
		)
	}

	t.Type = typeStruct

	return t
}

func Struct(fields ...StructField) (v *StructType) {
	return &StructType{
		fields: fields,
	}
}

func StructFields(ms []*Ydb.StructMember) []StructField {
	fs := make([]StructField, len(ms))
	for i, m := range ms {
		fs[i] = StructField{
			Name: m.Name,
			T:    TypeFromYDB(m.Type),
		}
	}
	return fs
}
