//go:build integration
// +build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/observability"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
)

func TestQuerySessionsClientSdkBuildInfoWithoutObservabilityChains(t *testing.T) {
	scope := newScope(t)

	const applicationName = "query-sessions-build-info-without-observability-chains"

	db := scope.Driver(
		ydb.WithApplicationName(applicationName),
		ydb.WithSessionPoolSizeLimit(1),
		spans.WithTraces(MockSpansAdapter{}),
	)

	const yql = "" +
		"DECLARE $sessionId AS Utf8;\n" +
		"DECLARE $applicationName AS Utf8;\n" +
		"SELECT ClientSdkBuildInfo, ApplicationName\n" +
		"FROM `.sys/query_sessions`\n" +
		"WHERE SessionId = $sessionId AND ApplicationName = $applicationName;"

	err := db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		row, err := s.QueryRow(ctx, yql,
			query.WithParameters(
				ydb.ParamsBuilder().
					Param("$sessionId").Text(s.ID()).
					Param("$applicationName").Text(applicationName).
					Build(),
			),
		)
		if err != nil {
			return err
		}

		var (
			clientSdkBuildInfo string
			gotApplicationName string
		)
		if err = row.Scan(&clientSdkBuildInfo, &gotApplicationName); err != nil {
			return err
		}

		require.Equal(t, applicationName, gotApplicationName)
		require.True(t, strings.HasPrefix(clientSdkBuildInfo, version.FullVersion), clientSdkBuildInfo)
		require.NotContains(t, clientSdkBuildInfo, observability.TracingChainName)
		require.NotContains(t, clientSdkBuildInfo, observability.MetricsChainName)

		return nil
	}, query.WithIdempotent())
	require.NoError(t, err)
}

func TestDiscoveryBuildInfoContainsObservabilityChains(t *testing.T) {
	scope := newScope(t)

	var discoveryBuildInfo string
	db := scope.Driver(
		spans.WithTraces(MockSpansAdapter{}),
		ydb.With(
			config.WithGrpcOptions(
				grpc.WithUnaryInterceptor(func(
					ctx context.Context,
					method string,
					req, reply any,
					cc *grpc.ClientConn,
					invoker grpc.UnaryInvoker,
					opts ...grpc.CallOption,
				) error {
					if strings.Contains(method, "ListEndpoints") {
						md, has := metadata.FromOutgoingContext(ctx)
						require.True(t, has)
						values := md.Get(meta.HeaderVersion)
						require.NotEmpty(t, values)
						discoveryBuildInfo = values[0]
					}

					return invoker(ctx, method, req, reply, cc, opts...)
				}),
			),
		),
	)

	_, err := db.Discovery().Discover(scope.Ctx)
	require.NoError(t, err)
	require.NotEmpty(t, discoveryBuildInfo)
	require.Contains(t, discoveryBuildInfo, version.FullVersion)
	require.Contains(t, discoveryBuildInfo, observability.TracingChainName+"/"+observability.TracingChainVersion)
}
