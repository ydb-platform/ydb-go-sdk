package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/connect"
)

func credentials() connect.Option {
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		return connect.WithAccessTokenCredentials(token)
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		return connect.WithAnonymousCredentials()
	}
	return func(ctx context.Context, client *connect.Connection) error {
		return nil
	}
}

func main() {
	ctx := context.Background()

	connectParams, err := connect.ConnectionString(os.Getenv("YDB"))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cannot create connect params from connection string env['YDB'] = '%s': %v\n", os.Getenv("YDB"), err)
		os.Exit(1)
	}

	connectCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	db, err := connect.New(
		connectCtx,
		connectParams,
		credentials(),
		connect.WithSessionPoolIdleThreshold(time.Second*5),
		connect.WithSessionPoolKeepAliveMinSize(-1),
		connect.WithDiscoveryInterval(5*time.Second),
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	err = db.CleanupDatabase(ctx, connectParams.Database(), "series", "episodes", "seasons")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cleaunup database failed: %v\n", err)
		os.Exit(1)
	}

	err = db.EnsurePathExists(ctx, connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ensure path exists failed: %v\n", err)
		os.Exit(1)
	}

	err = describeTableOptions(ctx, db.Table().Pool())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "describe table options error: %v\n", err)
		os.Exit(1)
	}

	err = createTables(ctx, db.Table().Pool(), connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create tables error: %v\n", err)
		os.Exit(1)
	}

	err = describeTable(ctx, db.Table().Pool(), path.Join(
		connectParams.Database(), "series",
	))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "describe table error: %v\n", err)
		os.Exit(1)
	}

	err = fillTablesWithData(ctx, db.Table().Pool(), connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "fill tables with data error: %v\n", err)
		os.Exit(1)
	}

	err = selectSimple(ctx, db.Table().Pool(), connectParams.Database())
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "select simple error: %v\n", err)
		os.Exit(1)
	}

	err = scanQuerySelect(ctx, db.Table().Pool(), connectParams.Database())
	if err != nil {
		if !ydb.IsTransportError(err, ydb.TransportErrorUnimplemented) {
			_, _ = fmt.Fprintf(os.Stderr, "scan query select error: %v\n", err)
			os.Exit(1)
		}
	}

	err = readTable(ctx, db.Table().Pool(), path.Join(
		connectParams.Database(), "series",
	))
	if err != nil {
		fmt.Printf("read table error: %v\n", err)
		os.Exit(1)
	}
}
