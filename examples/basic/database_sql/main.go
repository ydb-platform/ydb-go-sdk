package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
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
	db, err := sql.Open("ydb", dsn)
	if err != nil {
		log.Fatalf("connect error: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	db.SetConnMaxIdleTime(time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cc, err := ydb.Unwrap(db)
	if err != nil {
		panic(fmt.Errorf("unwrap failed: %w", err))
	}

	prefix = path.Join(cc.Name(), prefix)

	err = sugar.RemoveRecursive(ctx, cc, prefix)
	if err != nil {
		panic(fmt.Errorf("remove recursive failed: %w", err))
	}

	err = prepareSchema(ctx, db, prefix)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = fillTablesWithData(ctx, db, prefix)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}

	err = selectDefault(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	err = selectScan(ctx, db, prefix)
	if err != nil {
		panic(err)
	}
}
