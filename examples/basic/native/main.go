package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

var (
	dsn    string
	prefix string
)

func init() {
	required := []string{"ydb"}
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&dsn,
		"ydb", "",
		"YDB connection string",
	)
	flagSet.StringVar(&prefix,
		"prefix", "",
		"tables prefix",
	)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		flagSet.Usage()
		os.Exit(1)
	}
	flagSet.Visit(func(f *flag.Flag) {
		for i, arg := range required {
			if arg == f.Name {
				required = append(required[:i], required[i+1:]...)
			}
		}
	})
	if len(required) > 0 {
		fmt.Printf("\nSome required options not defined: %v\n\n", required)
		flagSet.Usage()
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := []ydb.Option{
		environ.WithEnvironCredentials(ctx),
	}
	db, err := ydb.Open(ctx, dsn, opts...)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix = path.Join(db.Name(), prefix)

	err = sugar.RemoveRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	err = sugar.MakeRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	err = describeTableOptions(ctx, db.Table())
	if err != nil {
		panic(fmt.Errorf("describe table options error: %w", err))
	}

	err = createTables(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = describeTable(ctx, db.Table(), path.Join(
		prefix, "series",
	))
	if err != nil {
		panic(fmt.Errorf("describe table error: %w", err))
	}

	err = fillTablesWithData(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}

	err = selectSimple(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("select simple error: %w", err))
	}

	err = scanQuerySelect(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("scan query select error: %w", err))
	}

	err = readTable(ctx, db.Table(), path.Join(
		prefix, "series",
	))
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}
}
