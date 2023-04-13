package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

var (
	dsn               string
	useEnvCredentials bool
)

func main() {
	readFlags()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var opts []ydb.Option
	if useEnvCredentials {
		opts = append(opts, environ.WithEnvironCredentials(ctx))
	}

	db, err := ydb.Open(
		ctx,
		dsn,
		opts...,
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix := path.Join(db.Name())
	tableName := "cdc"
	topicPath := tableName + "/feed"
	consumerName := "test-consumer"

	prepareTableWithCDC(ctx, db, prefix, tableName, topicPath, consumerName)

	go fillTable(ctx, db.Table(), prefix, tableName)
	go func() {
		time.Sleep(interval / 2)
		removeFromTable(ctx, db.Table(), prefix, tableName)
	}()

	cdcRead(ctx, db, consumerName, topicPath)
}

func readFlags() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		out := flagSet.Output()
		_, _ = fmt.Fprintf(out, "Usage:\n%s [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "\nOptions:\n")
		flagSet.PrintDefaults()
	}
	flagSet.StringVar(&dsn,
		"ydb", "grpc://localhost:2136/local",
		"YDB connection string",
	)
	flagSet.BoolVar(&useEnvCredentials,
		"use-env-credentials", false,
		"Use credentials from env variables",
	)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		flagSet.Usage()
		os.Exit(1)
	}
}

func prepareTableWithCDC(ctx context.Context, db *ydb.Driver, prefix, tableName, topicPath, consumerName string) {
	log.Println("Drop table (if exists)...")
	err := dropTableIfExists(
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
		prefix, tableName,
	)
	if err != nil {
		panic(fmt.Errorf("create table error: %w", err))
	}
	log.Println("Create table done")

	log.Println("Create consumer")
	err = db.Topic().Alter(ctx, topicPath, topicoptions.AlterWithAddConsumers(topictypes.Consumer{
		Name: consumerName,
	}))
	if err != nil {
		panic(fmt.Errorf("failed to create feed consumer: %w", err))
	}
}
