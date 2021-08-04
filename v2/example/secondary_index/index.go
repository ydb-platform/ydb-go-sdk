package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/example/internal/cli"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
)

const (
	TableSeries         = "series"
	TableSeriesRevViews = "series_rev_views"
)

var actions = map[string]func(context.Context, *table.SessionPool, string, ...string) error{
	"create":    doCreate,
	"generate":  doGenerate,
	"update":    doUpdate,
	"list":      doList,
	"listviews": doListViews,
	"delete":    doDelete,
	"drop":      doDrop,
}

type Command struct {
	config      func(cli.Parameters) *ydb.DriverConfig
	client      func() *table.Client
	sessionPool func() *table.SessionPool
}

func (cmd *Command) ExportFlags(ctx context.Context, flag *flag.FlagSet) {
	flag.Usage = func() {
		out := flag.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s command [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flag.PrintDefaults()
		_, _ = fmt.Fprintf(out, "\nCommands:\n")
		for c := range actions {
			_, _ = fmt.Fprintf(out, "  - %s\n", c)
		}
	}

	cmd.config = cli.ExportDriverConfig(ctx, flag)
	cmd.client = cli.ExportTableClient(flag)
	cmd.sessionPool = cli.ExportSessionPool(flag)
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	if len(params.Args) < 1 {
		fmt.Printf("no command specified.\n\n")
		return cli.ErrPrintUsage
	}
	var (
		name   = strings.ToLower(params.Args[0])
		action = actions[name]
	)
	if action == nil {
		fmt.Printf("unexpected command: %q\n\n", params.Args[0])
		return cli.ErrPrintUsage
	}

	driver, err := (&ydb.Dialer{
		DriverConfig: cmd.config(params),
	}).Dial(ctx, params.Endpoint)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer driver.Close()

	tableClient := cmd.client()
	tableClient.Driver = driver

	sessionPool := cmd.sessionPool()
	sessionPool.Builder = tableClient
	defer sessionPool.Close(ctx)

	prefix := path.Join(params.Database, params.Path)

	return action(ctx, sessionPool, prefix, params.Args[1:]...)
}
