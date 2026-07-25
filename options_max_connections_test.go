package ydb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithMaxConnections(t *testing.T) {
	driver, err := driverFromOptions(context.Background(), WithMaxConnections(13))
	require.NoError(t, err)
	t.Cleanup(driver.ctxCancel)

	require.Equal(t, 13, driver.config.Balancer().MaxConnections)
}
