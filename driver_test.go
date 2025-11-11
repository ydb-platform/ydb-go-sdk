package ydb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenWithExpiredContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(10 * time.Nanosecond)

	_, err := Open(ctx, "grpc://localhost:2136/local")
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "context deadline exceeded")
	require.Contains(t, err.Error(), "Open(driver.go:")
	// Verify that the error doesn't have multiple stack traces (no "retry failed on attempt")
	require.NotContains(t, err.Error(), "retry failed")
}

func TestOpenWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := Open(ctx, "grpc://localhost:2136/local")
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "context canceled")
	require.Contains(t, err.Error(), "Open(driver.go:")
	// Verify that the error doesn't have multiple stack traces (no "retry failed on attempt")
	require.NotContains(t, err.Error(), "retry failed")
}
