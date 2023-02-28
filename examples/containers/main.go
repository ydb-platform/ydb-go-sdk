package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
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

	err = db.Table().DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) (err error) {
			res, err := tx.Execute(ctx, render(query, nil), nil)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()

			parsers := [...]func() error{
				func() error {
					return res.Scan(&exampleList{})
				},
				func() error {
					return res.Scan(&exampleTuple{})
				},
				func() error {
					return res.Scan(&exampleDict{})
				},
				func() error {
					return res.Scan(&exampleStruct{})
				},
				func() error {
					return res.Scan(&variantStruct{})
				},
				func() error {
					return res.Scan(&variantTuple{})
				},
			}

			for set := 0; res.NextResultSet(ctx); set++ {
				res.NextRow()
				err = parsers[set]()
				if err != nil {
					return err
				}
			}
			return res.Err()
		})
	if err != nil {
		panic(err)
	}
}
