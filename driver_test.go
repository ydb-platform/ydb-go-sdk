package ydb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(10 * time.Nanosecond)

	_, err := Open(ctx, "grpc://localhost:2136/local")
	require.NoError(t, err)
}
