package connect

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2/scheme"
	"context"
)

type schemeWrapper struct {
	ctx    context.Context
	client *scheme.Client
}

func newSchemeWrapper(ctx context.Context) *schemeWrapper {
	return &schemeWrapper{
		ctx:    ctx,
		client: &scheme.Client{},
	}
}
