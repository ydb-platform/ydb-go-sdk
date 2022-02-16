package ydb

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"

const (
	MetaRequestType = meta.MetaRequestType
	MetaTraceID     = meta.MetaTraceID
)

func newMeta(meta meta.Meta, opts ...CustomOption) meta.Meta {
	if len(opts) == 0 {
		return meta
	}
	options := &customOptions{meta: meta}
	for _, opt := range opts {
		opt(options)
	}
	return options.meta
}
