package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
)

func TestWithMaxConnections(t *testing.T) {
	t.Run("nil balancer", func(t *testing.T) {
		cfg := &Config{}

		WithMaxConnections(17)(cfg)

		require.Equal(t, 17, cfg.Balancer().MaxConnections)
	})

	t.Run("copy configured balancer", func(t *testing.T) {
		source := &balancerConfig.Config{
			AllowFallback:  true,
			MaxConnections: 42,
		}
		cfg := New(
			WithBalancer(source),
			WithMaxConnections(7),
		)

		require.Equal(t, 7, cfg.Balancer().MaxConnections)
		require.True(t, cfg.Balancer().AllowFallback)
		require.Equal(t, 42, source.MaxConnections, "source balancer config must not be mutated")
	})

	t.Run("negative means unlimited", func(t *testing.T) {
		cfg := New(WithMaxConnections(-1))

		require.Zero(t, cfg.Balancer().MaxConnections)
	})

	t.Run("WithBalancer preserves MaxConnections", func(t *testing.T) {
		cfg := New(
			WithMaxConnections(11),
			WithBalancer(&balancerConfig.Config{AllowFallback: true}),
		)

		require.Equal(t, 11, cfg.Balancer().MaxConnections)
		require.True(t, cfg.Balancer().AllowFallback)
	})

	t.Run("WithBalancer keeps explicit MaxConnections from preset", func(t *testing.T) {
		cfg := New(
			WithMaxConnections(11),
			WithBalancer(&balancerConfig.Config{MaxConnections: 3}),
		)

		require.Equal(t, 3, cfg.Balancer().MaxConnections)
	})
}
