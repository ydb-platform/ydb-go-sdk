package types

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type Tagged struct {
	inner Type
	tag   string
}

func NewTagged(inner Type, tag string) *Tagged { return &Tagged{inner: inner, tag: tag} }
func (t *Tagged) InnerType() Type              { return t.inner }
func (t *Tagged) Tag() string                  { return t.tag }
func (t *Tagged) Yql() string                  { return fmt.Sprintf("Tagged<%s,'%s'>", t.inner.Yql(), t.tag) }
func (t *Tagged) String() string               { return t.Yql() }
func (t *Tagged) ToYDB() *Ydb.Type {
	return &Ydb.Type{Type: &Ydb.Type_TaggedType{TaggedType: &Ydb.TaggedType{Type: t.inner.ToYDB(), Tag: t.tag}}}
}

func (t *Tagged) equalsTo(other Type) bool {
	rhs, ok := other.(*Tagged)

	return ok && t.tag == rhs.tag && Equal(t.inner, rhs.inner)
}
