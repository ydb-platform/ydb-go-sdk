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
	db, err := ydb.Open(ctx, dsn,
		environ.WithEnvironCredentials(ctx),
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

	err = createTables(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = addDocument(ctx, db.Table(), prefix,
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		1)
	if err != nil {
		panic(fmt.Errorf("add document failed: %w", err))
	}

	err = addDocument(ctx, db.Table(), prefix,
		"https://ya.ru/",
		"<html><body><h1>Ya</h1></body></html>",
		2)
	if err != nil {
		panic(fmt.Errorf("add document failed: %w", err))
	}

	err = addDocument(ctx, db.Table(), prefix,
		"https://mail.yandex.ru/",
		"<html><body><h1>Mail</h1></body></html>",
		3)
	if err != nil {
		panic(fmt.Errorf("add document failed: %w", err))
	}

	err = addDocument(ctx, db.Table(), prefix,
		"https://zen.yandex.ru/",
		"<html><body><h1>Zen</h1></body></html>",
		4)
	if err != nil {
		panic(fmt.Errorf("add document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://ya.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://mail.yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://zen.yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = deleteExpired(ctx, db.Table(), prefix, 2)
	if err != nil {
		panic(fmt.Errorf("delete expired failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://ya.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://mail.yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://zen.yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = addDocument(ctx, db.Table(), prefix,
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		3)
	if err != nil {
		panic(fmt.Errorf("add document failed: %w", err))
	}

	err = addDocument(ctx, db.Table(), prefix,
		"https://ya.ru/",
		"<html><body><h1>Ya</h1></body></html>",
		4)
	if err != nil {
		panic(fmt.Errorf("add document failed: %w", err))
	}

	err = deleteExpired(ctx, db.Table(), prefix, 3)
	if err != nil {
		panic(fmt.Errorf("delete expired failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://ya.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://mail.yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}

	err = readDocument(ctx, db.Table(), prefix, "https://zen.yandex.ru/")
	if err != nil {
		panic(fmt.Errorf("read document failed: %w", err))
	}
}
