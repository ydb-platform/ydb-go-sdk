package main

import (
	"context"
	"database/sql"
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

	cc, err := ydb.Open(ctx,
		dsn,
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = cc.Close(ctx) }()

	prefix := path.Join(cc.Name(), "database_sql")

	c, err := ydb.Connector(cc,
		ydb.WithAutoDeclare(),
		ydb.WithTablePathPrefix(prefix),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	db := sql.OpenDB(c)
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	db.SetConnMaxIdleTime(time.Second)

	err = sugar.RemoveRecursive(ctx, cc, prefix)
	if err != nil {
		panic(fmt.Errorf("remove recursive failed: %w", err))
	}

	err = prepareSchema(ctx, db)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = fillTablesWithData(ctx, db)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}

	err = selectDefault(ctx, db)
	if err != nil {
		panic(err)
	}

	err = selectScan(ctx, db)
	if err != nil {
		panic(err)
	}
}
