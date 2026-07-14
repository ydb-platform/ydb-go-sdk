package ydb //nolint:testpackage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/observability"
)

func TestWithObservabilityBuildInfoChainOptions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		opts     []Option
		expected string
	}{
		{
			name: "tracing only",
			opts: []Option{
				WithObservabilityTracingBuildInfoChain(),
			},
			expected: version.FullVersion + " " +
				observability.TracingChainName + "/" + observability.TracingChainVersion,
		},
		{
			name: "metrics only",
			opts: []Option{
				WithObservabilityMetricsBuildInfoChain(),
			},
			expected: version.FullVersion + " " +
				observability.MetricsChainName + "/" + observability.MetricsChainVersion,
		},
		{
			name: "tracing and metrics",
			opts: []Option{
				WithObservabilityTracingBuildInfoChain(),
				WithObservabilityMetricsBuildInfoChain(),
			},
			expected: version.FullVersion + " " +
				observability.TracingChainName + "/" + observability.TracingChainVersion + ";" +
				observability.MetricsChainName + "/" + observability.MetricsChainVersion,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, err := driverFromOptions(context.Background(),
				append([]Option{
					WithDatabase("/local"),
				}, tt.opts...)...,
			)
			require.NoError(t, err)

			ctx, err := d.config.Meta().Context(context.Background())
			require.NoError(t, err)

			md, has := metadata.FromOutgoingContext(ctx)
			require.True(t, has)
			require.Equal(t, []string{tt.expected}, md.Get("x-ydb-sdk-build-info"))
		})
	}
}
