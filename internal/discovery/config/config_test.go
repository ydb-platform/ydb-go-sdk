package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGrpcChannelShutdownTimeout(t *testing.T) {
	require.Equal(t, 119*time.Second, New().GrpcChannelShutdownTimeout())
	require.Equal(t, 5*time.Second, New(WithGrpcChannelShutdownTimeout(5*time.Second)).GrpcChannelShutdownTimeout())
}
