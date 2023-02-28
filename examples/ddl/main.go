package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
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
	db, err := ydb.Open(ctx, dsn,
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix = path.Join(db.Name(), prefix)

	// simple creation with composite primary key
	err = executeQuery(ctx, db.Table(), prefix, simpleCreateQuery)
	if err != nil {
		panic(err)
	}

	// creation with column family
	err = executeQuery(ctx, db.Table(), prefix, familyCreateQuery)
	if err != nil {
		panic(err)
	}

	// creation with table settings
	err = executeQuery(ctx, db.Table(), prefix, settingsCreateQuery)
	if err != nil {
		panic(err)
	}

	// add column and drop column.
	err = executeQuery(ctx, db.Table(), prefix, alterQuery)
	if err != nil {
		panic(err)
	}

	// change AUTO_PARTITIONING_BY_SIZE setting.
	err = executeQuery(ctx, db.Table(), prefix, alterSettingsQuery)
	if err != nil {
		panic(err)
	}

	// add TTL. Clear the old data after the three-hour interval has expired.
	err = executeQuery(ctx, db.Table(), prefix, alterTTLQuery)
	if err != nil {
		panic(err)
	}

	// drop tables small_table,small_table2,small_table3.
	err = executeQuery(ctx, db.Table(), prefix, dropQuery)
	if err != nil {
		panic(err)
	}
}
