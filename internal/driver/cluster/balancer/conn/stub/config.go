package stub

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type configStub struct {
	config.Config
}

func (c configStub) Trace(ctx context.Context) trace.Driver {
	return c.Config.Trace().Compose(trace.ContextDriver(ctx))
}

func (c configStub) Pessimize(context.Context, endpoint.Endpoint) error {
	return nil
}

func Config(c config.Config) conn.Config {
	return &configStub{c}
}

func (c configStub) Meta(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
