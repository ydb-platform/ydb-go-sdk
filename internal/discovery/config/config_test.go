package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEndpointCloseDelay(t *testing.T) {
	require.Equal(t, 119*time.Second, New().EndpointCloseDelay())
	require.Equal(t, 5*time.Second, New(WithEndpointCloseDelay(5*time.Second)).EndpointCloseDelay())
}
