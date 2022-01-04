package config

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type stubConfig struct {
	config.Config
}

func (c stubConfig) Trace(ctx context.Context) trace.Driver {
	return c.Config.Trace().Compose(trace.ContextDriver(ctx))
}

func (c stubConfig) Pessimize(context.Context, endpoint.Endpoint) error {
	return nil
}

func Config(c config.Config) conn.Config {
	return &stubConfig{c}
}

func (c stubConfig) Meta(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
