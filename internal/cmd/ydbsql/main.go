package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/ydbsql"
)

func main() {
	connectParams := ydb.MustConnectionString(os.Getenv("YDB"))

	ctx := context.Background()

	opts := []ydbsql.ConnectorOption{
		ydbsql.WithConnectParams(connectParams),
		ydbsql.WithDefaultExecDataQueryOption(
			options.WithQueryCachePolicy(
				options.WithQueryCachePolicyKeepInCache(),
			),
		),
	}
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		opts = append(opts, ydbsql.WithCredentials(ydb.NewAuthTokenCredentials(token)))
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		opts = append(opts, ydbsql.WithCredentials(ydb.NewAnonymousCredentials()))
	}

	db := sql.OpenDB(ydbsql.Connector(opts...))
	defer func() { _ = db.Close() }()

	err := db.Ping()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ping failed: %v\n", err)
		os.Exit(1)
	}

	err = cleanupDatabase(ctx, db, "series", "episodes", "seasons")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cleaunup database failed: %v\n", err)
		os.Exit(1)
	}

	err = ensurePathExists(ctx, db)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ensure path exists failed: %v\n", err)
		os.Exit(1)
	}

	err = describeTableOptions(ctx, db)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "describe table options error: %v\n", err)
		os.Exit(1)
	}

	err = createTables(ctx, db, connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create tables error: %v\n", err)
		os.Exit(1)
	}

	err = describeTable(ctx, db, path.Join(
		connectParams.Database(), "series",
	))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "describe table error: %v\n", err)
		os.Exit(1)
	}

	err = fillTablesWithData(ctx, db, connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fill tables with data error: %v\n", err)
		os.Exit(1)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err = selectSimple(ctx, db, connectParams.Database())
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "select simple error: %v\n", err)
			}

			err = scanQuerySelect(ctx, db, connectParams.Database())
			if err != nil {
				if !errors.IsTransportError(err, errors.TransportErrorUnimplemented) {
					_, _ = fmt.Fprintf(os.Stderr, "scan query select error: %v\n", err)
				}
			}

			err = readTable(ctx, db, path.Join(
				connectParams.Database(), "series",
			))
			if err != nil {
				fmt.Printf("read table error: %v\n", err)
			}
		}()
	}
	wg.Wait()
}
