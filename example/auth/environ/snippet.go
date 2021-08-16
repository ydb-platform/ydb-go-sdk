package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2/connect"
	"context"
	"flag"
	"fmt"

	"github.com/yandex-cloud/ydb-go-sdk/v2/example/internal/cli"
)

type Command struct {
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(
		connectCtx,
		params.ConnectParams,
		// No need to specify other credentials options for use authenticate from environment variables
	)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	// work with db instance

	return nil
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}
