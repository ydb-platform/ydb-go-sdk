// +build integration

package test

import (
	"context"
	"database/sql"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"github.com/ydb-platform/ydb-go-sdk/v3/ydbsql"
	"log"
	"os"
)

func driverTrace() trace.Driver {
	var t trace.Driver
	trace.Stub(&t, func(name string, args ...interface{}) {
		log.Printf("[driver] %s: %+v", name, trace.ClearContext(args))
	})
	return t
}

func tableTrace() trace.Table {
	var t trace.Table
	trace.Stub(&t, func(name string, args ...interface{}) {
		log.Printf("[table] %s: %+v", name, trace.ClearContext(args))
	})
	return t
}

func appendConnectOptions(opts ...ydb.Option) []ydb.Option {
	opts = append(
		opts,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithTraceDriver(driverTrace()),
		ydb.WithTraceTable(tableTrace()),
	)
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		opts = append(opts, ydb.WithAccessTokenCredentials(token))
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		opts = append(opts, ydb.WithAnonymousCredentials())
	}
	return opts
}

func openNative(ctx context.Context, opts ...ydb.Option) (ydb.Connection, error) {
	return ydb.New(ctx, appendConnectOptions(opts...)...)
}

func openSql(ctx context.Context, opts ...ydbsql.sqlOption) (*sql.DB, error) {
	opts = append(
		opts,
		ydbsql.With(appendConnectOptions()...),
	)

	connector, err := ydbsql.Connector(opts...)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)

	return db, db.PingContext(ctx)
}
