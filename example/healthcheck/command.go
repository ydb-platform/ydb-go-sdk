package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/cli"
	"context"
	"flag"
	"fmt"
	"time"
)

type Command struct {
	urls string
}

func (cmd *Command) ExportFlags(ctx context.Context, flagSet *flag.FlagSet) {
	flagSet.StringVar(&cmd.urls, "urls", "", "URLs for check")
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) (err error) {
	service, err := NewService(ctx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("error on create service: %w", err)
	}
	fmt.Println(params.Args)
	defer service.Close()
	for {
		if err := service.check(ctx, params.Args); err != nil {
			return fmt.Errorf("error on check URLS [%v]: %w", params.Args, err)
		}
		select {
		case <-time.After(time.Minute):
			continue
		case <-ctx.Done():
			return nil
		}
	}
}
