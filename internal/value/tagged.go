package value

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

type taggedValue struct {
	t     *types.Tagged
	value Value
}

func (v *taggedValue) Type() types.Type {
	return v.t
}

func (v *taggedValue) Yql() string {
	return fmt.Sprintf("AsTagged(%s,%q)", v.value.Yql(), v.t.Tag())
}

func (v *taggedValue) castTo(dst any) error {
	return v.value.castTo(dst)
}

func (v *taggedValue) toYDB() *Ydb.Value {
	return v.value.toYDB()
}
