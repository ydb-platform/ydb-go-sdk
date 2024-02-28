package query

import "github.com/ydb-platform/ydb-go-sdk/v3/table/types"

type Column interface {
	Name() string
	Type() types.Type
}
