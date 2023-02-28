package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
)

var (
	dsn       string
	prefix    string
	tablePath string
	count     int
)

func init() {
	required := []string{"ydb", "table"}
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
	flagSet.IntVar(&count,
		"count", 1000,
		"count requests",
	)
	flagSet.StringVar(&tablePath,
		"table", "bulk_upsert_example",
		"Path for table",
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
		environ.WithEnvironCredentials(ctx),
	)

	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	tablePath = path.Join(db.Name(), prefix, tablePath)

	err = sugar.RemoveRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}
	err = sugar.MakeRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	if err := createLogTable(ctx, db.Table(), tablePath); err != nil {
		panic(wrap(err, "failed to create table"))
	}
	var logs []logMessage
	for offset := 0; offset < count; offset++ {
		logs = getLogBatch(logs, offset)
		if err := writeLogBatch(ctx, db.Table(), tablePath, logs); err != nil {
			panic(wrap(err, fmt.Sprintf("failed to write batch offset %d", offset)))
		}
		fmt.Print(".")
	}
	log.Print("Done.\n")
}
