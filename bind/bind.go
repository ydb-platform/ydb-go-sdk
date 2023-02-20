package bind

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/bind"
)

type Binding = bind.Binding

func TablePathPrefix(tablePathPrefix string) Binding {
	return bind.WithTablePathPrefix(tablePathPrefix)
}

func Params() Binding {
	return bind.WithAutoBindParams()
}
