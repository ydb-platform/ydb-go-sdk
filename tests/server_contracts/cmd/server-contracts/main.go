package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ydb-platform/ydb-go-sdk/v3/tests/server_contracts/internal/runner"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(runner.Run(ctx, os.Stdin, os.Stdout, os.Stderr, os.Args[1:]))
}
