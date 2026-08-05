package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigStringIncludesMaxConnections(t *testing.T) {
	cfg := Config{
		AllowFallback:   true,
		DetectNearestDC: true,
		MaxConnections:  9,
	}

	require.Equal(
		t,
		"RandomChoice{DetectNearestDC=true,AllowFallback=true,MaxConnections=9}",
		cfg.String(),
	)
}
