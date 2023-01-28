//go:build !fast
// +build !fast

package ydb_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
)

func ExampleOpen() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2135/local")
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
		"grpc://localhost:2135/local",
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

func TestZeroDialTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	db, err := ydb.Open(
		ctx,
		"grpc://non-existent.com:2135/some",
		ydb.WithDialTimeout(0),
	)

	require.Error(t, err)
	require.Nil(t, db)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || ydb.IsTransportError(err, grpcCodes.DeadlineExceeded))
}

func TestClusterDiscoveryRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	counter := 0

	db, err := ydb.Open(ctx,
		"grpc://non-existent.com:2135/some",
		ydb.WithDialTimeout(time.Second),
		ydb.WithTraceDriver(trace.Driver{
			OnBalancerUpdate: func(info trace.DriverBalancerUpdateStartInfo) func(trace.DriverBalancerUpdateDoneInfo) {
				counter++
				return nil
			},
		}),
	)
	t.Logf("attempts: %d", counter)
	require.Error(t, err)
	require.Nil(t, db)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || ydb.IsTransportError(err, grpcCodes.DeadlineExceeded))
	require.Greater(t, counter, 1)
}
