package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

var dsn string

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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cc, err := ydb.Open(ctx, dsn)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = cc.Close(ctx) }()

	c, err := ydb.Connector(cc,
		ydb.WithAutoDeclare(),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	db := sql.OpenDB(c)
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	db.SetConnMaxIdleTime(time.Second)

	err = sugar.RemoveRecursive(ctx, cc, "")
	if err != nil {
		panic(fmt.Errorf("remove recursive failed: %w", err))
	}

	err = prepareSchema(ctx, db)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = fillTablesWithData(ctx, db)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}

	err = selectDefault(ctx, db)
	if err != nil {
		panic(err)
	}

	err = selectScan(ctx, db)
	if err != nil {
		panic(err)
	}
}
