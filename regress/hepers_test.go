//go:build !fast
// +build !fast

package regress

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func connect(t *testing.T) ydb.Connection {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	connectionString := os.Getenv("YDB_CONNECTION_STRING")

	if connectionString == "" {
		connectionString = "grpc://localhost:2136?database=/local"
	}

	c, err := ydb.Open(ctx, connectionString)
	require.NoError(t, err)
	return c
}
