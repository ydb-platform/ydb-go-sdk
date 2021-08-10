package connect

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/scheme"
	"context"
	"sync"
)

type schemeWrapper struct {
	ctx              context.Context
	driver           ydb.Driver
	schemeClientOnce sync.Once
	schemeClient     *scheme.Client
}

func newSchemeWrapper(ctx context.Context, driver ydb.Driver) *schemeWrapper {
	return &schemeWrapper{
		ctx:    ctx,
		driver: driver,
	}
}

func (s *schemeWrapper) singleton() *scheme.Client {
	s.schemeClientOnce.Do(func() {
		s.schemeClient = &scheme.Client{
			Driver: s.driver,
		}
	})
	return s.schemeClient
}
