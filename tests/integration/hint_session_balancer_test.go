//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"google.golang.org/grpc/metadata"
)

func TestSQL(t *testing.T) {
	// TestSQL tests the behavior of server-side session balancing in both SQL and native YDB drivers.
	// It verifies that the "session-balancer" capability is correctly added or removed based on configuration.
	// The test covers four scenarios:
	// 1. SQL driver with server balancer disabled
	// 2. SQL driver with server balancer enabled
	// 3. Native driver with server balancer disabled
	// 4. Native driver with server balancer enabled

	t.Run("sql driver with disable server balancer", func(t *testing.T) {
		var (
			scope = newScope(t)
			caps  []string
			db    = sqlDriverWithOnSessionCreateTraces(scope, &caps,
				ydb.WithDisableServerBalancer(),
			)
		)

		db.ExecContext(scope.Ctx, "SELECT 42")
		scope.Require.NotContains(caps, "session-balancer")
	})

	t.Run("sql driver without disable server balancer", func(t *testing.T) {
		var (
			scope = newScope(t)
			caps  []string
			db    = sqlDriverWithOnSessionCreateTraces(scope, &caps)
		)

		db.ExecContext(scope.Ctx, "SELECT 42")
		scope.Require.Contains(caps, "session-balancer")
	})

	t.Run("native driver with disable server balancer", func(t *testing.T) {
		var (
			scope = newScope(t)
			caps  []string
			db    = nativeDriverWithYDBCapabilitiesTraces(scope, &caps,
				ydb.WithDisabledServerBalancer(),
			)
		)

		db.Query().Exec(scope.Ctx, "SELECT 42")
		scope.Require.NotContains(caps, "session-balancer")
	})

	t.Run("native driver without disable server balancer", func(t *testing.T) {
		var (
			scope = newScope(t)
			caps  []string
			db    = nativeDriverWithYDBCapabilitiesTraces(scope, &caps)
		)

		db.Query().Exec(scope.Ctx, "SELECT 42")
		scope.Require.Contains(caps, "session-balancer")
	})
}

// internal

func extractYDBCapabilities(mdCtx context.Context) []string {
	md, _ := metadata.FromOutgoingContext(mdCtx)
	return md.Get("x-ydb-client-capabilities")
}

func sqlDriverWithOnSessionCreateTraces(scope *scopeT, capabilities *[]string, opts ...ydb.ConnectorOption) *sql.DB {
	scope.T().Helper()

	nativeDriver := nativeDriverWithYDBCapabilitiesTraces(scope, capabilities)

	// only query mode testing
	opts = append(opts, ydb.WithDefaultQueryMode(ydb.QueryExecuteQueryMode))

	connector, err := ydb.Connector(nativeDriver, opts...)
	scope.Require.NoError(err)

	return sql.OpenDB(connector)
}

func nativeDriverWithYDBCapabilitiesTraces(scope *scopeT, capabilities *[]string, opts ...ydb.Option) *ydb.Driver {
	scope.T().Helper()

	opts = append(opts, ydb.WithTraceQuery(trace.Query{
		OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(trace.QuerySessionCreateDoneInfo) {
			*capabilities = extractYDBCapabilities(*info.Context)

			return func(trace.QuerySessionCreateDoneInfo) {}
		}}))

	return scope.Driver(opts...)
}
