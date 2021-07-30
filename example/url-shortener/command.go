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

func (cmd *Command) Run(ctx context.Context, parameters cli.Parameters) error {
	h, err := NewURLShortener(ctx, parameters.Endpoint, parameters.Database, parameters.TLS)
	if err != nil {
		return fmt.Errorf("error on create handler: %w", err)
	}
	defer h.Close()
	return http.ListenAndServe(":"+strconv.Itoa(cmd.port), http.HandlerFunc(h.Handle))
}

func (cmd *Command) ExportFlags(ctx context.Context, flagSet *flag.FlagSet) {
	flagSet.IntVar(&cmd.port, "port", 80, "http port for web-server")
}
