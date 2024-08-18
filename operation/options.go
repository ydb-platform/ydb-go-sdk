package operation

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation/options"
)

func WithKind(kind string) options.List {
	return func(r *options.ListOperationsRequest) {
		r.Kind = kind
	}
}
