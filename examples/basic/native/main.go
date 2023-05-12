package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn, exists := os.LookupEnv("YDB_CONNECTION_STRING")
	if !exists {
		panic("YDB_CONNECTION_STRING environment variable not defined")
	}

	db, err := ydb.Open(ctx,
		dsn,
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix := path.Join(db.Name(), "native")

	err = sugar.RemoveRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	err = describeTableOptions(ctx, db.Table())
	if err != nil {
		panic(fmt.Errorf("describe table options error: %w", err))
	}

	err = createTables(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = describeTable(ctx, db.Table(), path.Join(prefix, "series"))
	if err != nil {
		panic(fmt.Errorf("describe table error: %w", err))
	}

	err = fillTablesWithData(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}

	err = selectSimple(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("select simple error: %w", err))
	}

	err = scanQuerySelect(ctx, db.Table(), prefix)
	if err != nil {
		panic(fmt.Errorf("scan query select error: %w", err))
	}

	err = readTable(ctx, db.Table(), path.Join(prefix, "series"))
	if err != nil {
		panic(fmt.Errorf("read table error: %w", err))
	}
}
