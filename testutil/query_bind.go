package testutil

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
)

type QueryBindings = query.Bindings

func QueryBind(bindings ...query.Bind) query.Bindings {
	return bindings
}
