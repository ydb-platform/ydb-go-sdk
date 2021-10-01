package test

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"os"
	"testing"
)

func OpenDB(t *testing.T, ctx context.Context, opts ...ydb.Option) ydb.Connection {
	if token, has := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); has {
		opts = append(opts, ydb.WithAccessTokenCredentials(token))
	}
	if v, has := os.LookupEnv("YDB_ANONYMOUS_CREDENTIALS"); has && v == "1" {
		opts = append(opts, ydb.WithAnonymousCredentials())
	}

	db, err := ydb.New(
		ctx,
		ydb.EndpointDatabase(
			os.Getenv("YDB_ENDPOINT"),
			os.Getenv("YDB_DATABASE"),
			func() bool {
				if v, ok := os.LookupEnv("YDB_SECURE_CONNECTION"); ok && v == "1" {
					return true
				}
				return false
			}(),
		),
		opts...,
	)
	if err != nil {
		t.Fatalf("connect error: %v\n", err)
	}

	return db
}
