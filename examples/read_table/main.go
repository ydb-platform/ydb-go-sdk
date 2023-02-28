package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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

	tableName := "orders"

	log.Println("Drop table (if exists)...")
	err = dropTableIfExists(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
	)
	if err != nil {
		panic(fmt.Errorf("drop table error: %w", err))
	}
	log.Println("Drop table done")

	log.Println("Create table...")
	err = createTable(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
	)
	if err != nil {
		panic(fmt.Errorf("create table error: %w", err))
	}
	log.Println("Create table done")

	log.Println("Fill table...")
	err = fillTable(
		ctx,
		db.Table(),
		prefix,
	)
	if err != nil {
		panic(fmt.Errorf("fill table error: %w", err))
	}
	log.Println("Fill table done")

	log.Println("Read whole table, unsorted:")
	err = readTable(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
	)
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}

	log.Println("Sorted by composite primary key:")
	err = readTable(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
		options.ReadOrdered(),
	)
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}

	log.Println("Any five rows:")
	err = readTable(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
		options.ReadRowLimit(5),
	)
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}

	log.Println("First five rows by PK (ascending) with subset of columns:")
	err = readTable(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
		options.ReadRowLimit(5),
		options.ReadColumn("customer_id"),
		options.ReadColumn("order_id"),
		options.ReadColumn("order_date"),
		options.ReadOrdered(),
	)
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}

	log.Println("Read all rows with first PK component (customer_id,) greater or equal than 2 and less then 3:")
	keyRange := options.KeyRange{
		From: types.TupleValue(
			types.OptionalValue(types.Uint64Value(2)),
		),
		To: types.TupleValue(
			types.OptionalValue(types.Uint64Value(3)),
		),
	}
	err = readTable(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
		options.ReadKeyRange(keyRange),
	)
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}

	log.Println("Read all rows with composite PK lexicographically less or equal than (1,4):")
	err = readTable(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
		options.ReadLessOrEqual(
			types.TupleValue(
				types.OptionalValue(types.Uint64Value(1)),
				types.OptionalValue(types.Uint64Value(4)),
			),
		),
	)
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}

	log.Println("Read all rows with composite PK lexicographically greater or equal than (1,2) and less than (3,4):")
	keyRange = options.KeyRange{
		From: types.TupleValue(
			types.OptionalValue(types.Uint64Value(1)),
			types.OptionalValue(types.Uint64Value(2)),
		),
		To: types.TupleValue(
			types.OptionalValue(types.Uint64Value(3)),
			types.OptionalValue(types.Uint64Value(1)),
		),
	}
	err = readTable(
		ctx,
		db.Table(),
		path.Join(prefix, tableName),
		options.ReadKeyRange(keyRange),
	)
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}
}
