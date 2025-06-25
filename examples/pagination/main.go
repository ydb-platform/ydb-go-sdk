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

func init() { //nolint:gochecknoinits
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
	db, err := ydb.Open(ctx, dsn,
		environ.WithEnvironCredentials(),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	err = sugar.RemoveRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}
	err = sugar.MakeRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	prefix = path.Join(db.Name(), prefix)

	err = createTable(ctx, db.Table(), path.Join(prefix, "schools"))
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = fillTableWithData(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}

	var lastNum uint
	lastCity := ""
	limit := 3
	maxPages := 10
	for i, empty := 0, false; i < maxPages && !empty; i++ {
		fmt.Printf("> Page %v:\n", i+1)
		empty, err = selectPaging(ctx, db.Table(), prefix, limit, &lastNum, &lastCity)
		if err != nil {
			panic(fmt.Errorf("get page %v error: %w", i, err))
		}
	}
}
