package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type quet struct {
}

func (q quet) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func Prepare(ctx context.Context, db ydb.Connection) error {
	err := db.Scheme().CleanupDatabase(ctx, db.Name(), "series", "episodes", "seasons")
	if err != nil {
		return fmt.Errorf("cleaunup database failed: %w\n", err)
	}

	err = db.Scheme().EnsurePathExists(ctx, db.Name())
	if err != nil {
		return fmt.Errorf("ensure path exists failed: %w\n", err)
	}

	err = describeTableOptions(ctx, db.Table())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "describe table options error: %v\n", err)
		os.Exit(1)
	}

	err = createTables(ctx, db.Table(), db.Name())
	if err != nil {
		return fmt.Errorf("create tables error: %w\n", err)
	}

	err = describeTable(ctx, db.Table(), path.Join(db.Name(), "series"))
	if err != nil {
		return fmt.Errorf("describe table error: %w\n", err)
	}

	return nil
}

func Select(ctx context.Context, db ydb.Connection) error {
	err := selectSimple(ctx, db.Table(), db.Name())
	if err != nil {
		return fmt.Errorf("select simple error: %w\n", err)
	}

	err = scanQuerySelect(ctx, db.Table(), db.Name())
	if err != nil {
		return fmt.Errorf("scan query error: %w\n", err)
	}

	err = readTable(ctx, db.Table(), path.Join(db.Name(), "series"))
	if err != nil {
		return fmt.Errorf("read table error: %w\n", err)
	}

	return nil
}

func Quet() {
	log.SetOutput(&quet{})
}

func main() {
	ctx := context.Background()

	connectParams, err := ydb.ConnectionString(os.Getenv("YDB"))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cannot create connect params from connection string env['YDB'] = '%s': %v\n", os.Getenv("YDB"), err)
		os.Exit(1)
	}

	opts := []ydb.Option{
		ydb.WithDialTimeout(5 * time.Second),
		ydb.WithGrpcConnectionTTL(5 * time.Second),
		ydb.WithSessionPoolIdleThreshold(time.Second * 5),
		ydb.WithSessionPoolKeepAliveMinSize(-1),
		ydb.WithDiscoveryInterval(5 * time.Second),
	}
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		opts = append(opts, ydb.WithAccessTokenCredentials(token))
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		opts = append(opts, ydb.WithAnonymousCredentials())
	}

	db, err := ydb.New(
		ctx,
		connectParams,
		opts...,
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	err = Prepare(ctx, db)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		os.Exit(1)
	}

	err = Fill(ctx, db.Table(), db.Name())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fill tables with data error: %w\n", err)
	}

	Quet()

	concurrency := 200
	wg := sync.WaitGroup{}
	fmt.Printf("grpc version: %v\n", grpc.Version)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				fmt.Print(".")

				err := Select(ctx, db)
				if err != nil {
					// nolint:staticcheck
					// ignore SA1019
					// We want to check internal grpc error on chaos monkey testing
					if errors.Is(err, grpc.ErrClientConnClosing) {
						panic(err)
					}
					if errors.Is(err, context.Canceled) {
						fmt.Println("exit")
						return
					}
				}
			}
		}()
	}

	wg.Wait()
}
