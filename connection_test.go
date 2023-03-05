//go:build !fast
// +build !fast

package ydb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Check *Driver is implementation of Connection
var _ ydb.Connection = &ydb.Driver{}

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
			OnBalancerClusterDiscoveryAttempt: func(info trace.DriverBalancerClusterDiscoveryAttemptStartInfo) func(
				trace.DriverBalancerClusterDiscoveryAttemptDoneInfo,
			) {
				counter++
				return nil
			},
		}),
	)
	t.Logf("attempts: %d", counter)
	t.Logf("err: %v", err)
	require.Error(t, err)
	require.Nil(t, db)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || ydb.IsTransportError(err, grpcCodes.DeadlineExceeded))
	require.Greater(t, counter, 1)
}
