package testutil

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
)

type QueryBindings = bind.Bindings

func QueryBind(bindings ...bind.Bind) bind.Bindings {
	return bind.Sort(bindings)
}
