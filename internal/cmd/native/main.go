package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"os"
	"path"
	"sync"
	"time"

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

func main() {
	ctx := context.Background()

	connectParams, err := ydb.ConnectionString(os.Getenv("YDB"))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cannot create connect params from connection string env['YDB'] = '%s': %v\n", os.Getenv("YDB"), err)
		os.Exit(1)
	}

	opts := []ydb.Option{
		ydb.WithDialTimeout(5 * time.Second),
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

	err = db.Scheme().CleanupDatabase(ctx, connectParams.Database(), "series", "episodes", "seasons")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cleaunup database failed: %v\n", err)
		os.Exit(1)
	}

	err = db.Scheme().EnsurePathExists(ctx, connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ensure path exists failed: %v\n", err)
		os.Exit(1)
	}

	err = describeTableOptions(ctx, db.Table())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "describe table options error: %v\n", err)
		os.Exit(1)
	}

	err = createTables(ctx, db.Table(), connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create tables error: %v\n", err)
		os.Exit(1)
	}

	err = describeTable(ctx, db.Table(), path.Join(
		connectParams.Database(), "series",
	))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "describe table error: %v\n", err)
		os.Exit(1)
	}

	err = fillTablesWithData(ctx, db.Table(), connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fill tables with data error: %v\n", err)
		os.Exit(1)
	}

	log.SetOutput(&quet{})

	concurrency := 200
	wg := sync.WaitGroup{}
	fmt.Printf("grpc version: %v\n", grpc.Version)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				fmt.Print(".")

				err = selectSimple(ctx, db.Table(), connectParams.Database())
				if err != nil {
					if errors.Is(err, grpc.ErrClientConnClosing) {
						panic(err)
					}
					if errors.Is(err, context.Canceled) {
						fmt.Println("exit")
						return
					}
				}

				err = scanQuerySelect(ctx, db.Table(), connectParams.Database())
				if err != nil {
					if errors.Is(err, grpc.ErrClientConnClosing) {
						panic(err)
					}
					if errors.Is(err, context.Canceled) {
						fmt.Println("exit")
						return
					}
				}

				err = readTable(ctx, db.Table(), path.Join(
					connectParams.Database(), "series",
				))
				if err != nil {
					if errors.Is(err, grpc.ErrClientConnClosing) {
						panic(err)
					}
					if errors.Is(err, context.Canceled) {
						fmt.Println("exit")
						return
					}
				}

				log.Printf("> cluster stats:\n")
				for e, s := range db.Stats() {
					log.Printf("  > '%v': %v\n", e, s)
				}

				whoAmI, err := db.Discovery().WhoAmI(ctx)
				log.Printf("whoAmI: %v, %v\n", whoAmI, err)
				if err != nil {
					if errors.Is(err, grpc.ErrClientConnClosing) {
						panic(err)
					}
					if errors.Is(err, context.Canceled) {
						fmt.Println("exit")
						return
					}
				} else {
					log.Printf("whoAmI: %v, %v\n", whoAmI, err)
				}
			}
		}()
	}

	wg.Wait()
}
