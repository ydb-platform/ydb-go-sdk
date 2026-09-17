package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
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
		opts = append(opts, environ.WithEnvironCredentials())
	}
	opts = append(opts, ydb.WithLazyTx(true))

	db, err := ydb.Open(
		ctx,
		dsn,
		opts...,
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = db.Close(closeCtx)
	}()

	prefix := path.Join(db.Name())
	tableName := "cdc"
	topicPath := tableName + "/feed"
	consumerName := "test-consumer"

	prepareTableWithCDC(ctx, db, prefix, tableName, topicPath, consumerName)

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	run := func(operation func() error) {
		wg.Go(func() {
			errCh <- operation()
		})
	}
	run(func() error {
		return fillTable(ctx, db.Table(), prefix, tableName)
	})
	run(func() error {
		timer := time.NewTimer(interval / 2)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return removeFromTable(ctx, db.Table(), prefix, tableName)
		}
	})
	run(func() error {
		return cdcRead(ctx, db, consumerName, topicPath)
	})

	err = <-errCh
	cancel()
	wg.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}
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
