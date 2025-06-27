//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestWithDisableServerBalancer(t *testing.T) {
	// This test verifies that the "session-balancer" capability is correctly added or removed based on configuration.
	// The test covers four scenarios:
	// 1. SQL driver with server balancer disabled
	// 2. SQL driver with server balancer enabled
	// 3. Native driver with server balancer disabled
	// 4. Native driver with server balancer enabled
	//
	// All cases test in two modes: query and table.

	var scope = newScope(t)

	// Test matrix
	tests := []struct {
		nativeEnable  bool // Add/Remove `ydb.WithDisableServerBalancer()` for native driver
		sqlEnable     bool // Add/Remove `ydb.WithDisableServerBalancer()` for SQL driver
		nativeHasHint bool
		sqlHasHint    bool
	}{
		{
			nativeEnable:  false,
			sqlEnable:     false,
			nativeHasHint: true,
			sqlHasHint:    true,
		},
		{
			nativeEnable:  true,
			sqlEnable:     false,
			nativeHasHint: false,
			sqlHasHint:    false,
		},
		{
			nativeEnable:  false,
			sqlEnable:     true,
			nativeHasHint: true,
			sqlHasHint:    false,
		},
		{
			nativeEnable:  true,
			sqlEnable:     true,
			nativeHasHint: false,
			sqlHasHint:    false,
		},
	}

	for _, mode := range []string{"query", "table"} {
		for _, test := range tests {
			testName := fmt.Sprintf("native=%t,sql=%t,checking=%s", test.nativeEnable, test.sqlEnable, mode)
			t.Run(testName, func(t *testing.T) {
				var (
					nativeCapabilities []string
					sqlCapabilities    []string
					nativeOpts         = []ydb.Option{ydbTraceCapabilitiesOpt(&nativeCapabilities)}
					nativeForSQLOpts   = []ydb.Option{ydbTraceCapabilitiesOpt(&sqlCapabilities)}
					sqlOpts            = []ydb.ConnectorOption{}
					err                error
				)

				const q = "SELECT 42"

				if test.nativeEnable {
					nativeOpts = append(nativeOpts, ydb.WithDisableServerBalancer())
					nativeForSQLOpts = append(nativeForSQLOpts, ydb.WithDisableServerBalancer())
				}

				if test.sqlEnable {
					sqlOpts = append(sqlOpts, ydb.WithDisableServerBalancer())
				}

				if mode == "query" {
					sqlOpts = append(sqlOpts, ydb.WithDefaultQueryMode(ydb.QueryExecuteQueryMode))
				}

				native := scope.NonCachingDriver(nativeOpts...)
				if mode == "query" {
					err = native.Query().Exec(scope.Ctx, q)
				} else {
					err = native.Table().Do(scope.Ctx,
						func(ctx context.Context, s table.Session) error {
							_, _, err := s.Execute(ctx, table.DefaultTxControl(), q, nil)
							return err
						})
				}
				scope.Require.NoError(err)

				connector, err := ydb.Connector(scope.NonCachingDriver(nativeForSQLOpts...), sqlOpts...)
				scope.Require.NoError(err)

				_, err = sql.OpenDB(connector).Exec(q)
				scope.Require.NoError(err)

				if test.nativeHasHint {
					scope.Require.Contains(nativeCapabilities, "session-balancer")
				} else {
					scope.Require.NotContains(nativeCapabilities, "session-balancer")
				}

				if test.sqlHasHint {
					scope.Require.Contains(sqlCapabilities, "session-balancer")
				} else {
					scope.Require.NotContains(sqlCapabilities, "session-balancer")
				}
			})
		}
	}
}

// internal

func ydbTraceCapabilitiesOpt(capabilities *[]string) ydb.Option {
	return ydb.WithTraceDriver(trace.Driver{
		OnBalancerChooseEndpoint: func(info trace.DriverBalancerChooseEndpointStartInfo) func(trace.DriverBalancerChooseEndpointDoneInfo) {
			*capabilities = extractYDBCapabilities(*info.Context)

			fmt.Println("capabilities:", info.Call.String())

			return func(trace.DriverBalancerChooseEndpointDoneInfo) {}
		},
	})
}

func extractYDBCapabilities(mdCtx context.Context) []string {
	md, _ := metadata.FromOutgoingContext(mdCtx)
	return md.Get("x-ydb-client-capabilities")
}
