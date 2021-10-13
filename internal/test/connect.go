package test

import (
	"context"
	"os"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func OpenDB(ctx context.Context, t *testing.T, opts ...ydb.Option) ydb.Connection {
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		opts = append(opts, ydb.WithAccessTokenCredentials(token))
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		opts = append(opts, ydb.WithAnonymousCredentials())
	}

	db, err := ydb.New(
		ctx,
		ydb.MustConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		opts...,
	)
	if err != nil {
		t.Fatalf("connect error: %v\n", err)
	}

	return db
}
