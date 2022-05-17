//go:build !fast
// +build !fast

package ydb_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
)

func ExampleOpen() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2135?database=/local")
	if err != nil {
		fmt.Printf("connection failed: %v", err)
	}
	defer db.Close(ctx) // cleanup resources
	fmt.Printf("connected to %s, database '%s'", db.Endpoint(), db.Name())
}

func ExampleOpen_advanced() {
	ctx := context.TODO()
	db, err := ydb.Open(
		ctx,
		"grpc://localhost:2135?database=/local",
		ydb.WithAnonymousCredentials(),
		ydb.WithBalancer(
			balancers.PreferLocationsWithFallback(
				balancers.RandomChoice(), "a", "b",
			),
		),
		ydb.WithSessionPoolSizeLimit(100),
	)
	if err != nil {
		fmt.Printf("connection failed: %v", err)
	}
	defer db.Close(ctx) // cleanup resources
	fmt.Printf("connected to %s, database '%s'", db.Endpoint(), db.Name())
}
