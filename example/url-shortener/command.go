package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/cli"
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
)

type Command struct {
	port int
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	service, err := NewService(ctx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("error on create service: %w", err)
	}
	defer service.Close()
	return http.ListenAndServe(":"+strconv.Itoa(cmd.port), http.HandlerFunc(service.Router))
}

func (cmd *Command) ExportFlags(ctx context.Context, flagSet *flag.FlagSet) {
	flagSet.IntVar(&cmd.port, "port", 80, "http port for web-server")
}
