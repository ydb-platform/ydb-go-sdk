package ydb //nolint:testpackage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/observability"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
)

func TestWithObservabilityBuildInfoChains(t *testing.T) {
	for _, tt := range []struct {
		name              string
		opts              []Option
		expectedRegular   string
		expectedDiscovery string
	}{
		{
			name: "tracing only",
			opts: []Option{
				With(config.WithBuildInfo(observability.TracingChainName, observability.TracingChainVersion)),
			},
			expectedRegular: version.FullVersion,
			expectedDiscovery: version.FullVersion + ";" +
				observability.TracingChainName + "/" + observability.TracingChainVersion,
		},
		{
			name: "metrics only",
			opts: []Option{
				With(config.WithBuildInfo(observability.MetricsChainName, observability.MetricsChainVersion)),
			},
			expectedRegular: version.FullVersion,
			expectedDiscovery: version.FullVersion + ";" +
				observability.MetricsChainName + "/" + observability.MetricsChainVersion,
		},
		{
			name: "tracing and metrics",
			opts: []Option{
				With(config.WithBuildInfo(observability.TracingChainName, observability.TracingChainVersion)),
				With(config.WithBuildInfo(observability.MetricsChainName, observability.MetricsChainVersion)),
			},
			expectedRegular: version.FullVersion,
			expectedDiscovery: version.FullVersion + ";" +
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
			require.Equal(t, []string{tt.expectedRegular}, md.Get("x-ydb-sdk-build-info"))

			discoveryCtx, err := d.config.Meta().DiscoveryContext(context.Background())
			require.NoError(t, err)

			discoveryMD, has := metadata.FromOutgoingContext(discoveryCtx)
			require.True(t, has)
			require.Equal(t, []string{tt.expectedDiscovery}, discoveryMD.Get("x-ydb-sdk-build-info"))
		})
	}
}
