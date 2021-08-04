package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2/example/internal/cli"
	"context"
	"flag"
	"fmt"
)

type Command struct {
}

func (cmd *Command) ExportFlags(ctx context.Context, flagSet *flag.FlagSet) {
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) (err error) {
	h, err := NewHealthcheck(ctx, params.Endpoint, params.Database, params.TLS)
	if err != nil {
		return fmt.Errorf("error on create healthcheck: %w", err)
	}
	defer h.Close()
	return h.check(ctx, params.Args)
}

