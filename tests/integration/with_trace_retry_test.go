//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestWithTraceRetry(t *testing.T) {
	ctx := xtest.Context(t)

	t.Run("table", func(t *testing.T) {
		var (
			retryCalled = make(map[string]bool, 2)
			scope       = newScope(t)
			db          = scope.Driver(
				ydb.WithTraceRetry(trace.Retry{
					OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
						retryCalled[info.Label] = true
						return nil
					},
				}),
			)
		)

		require.NoError(t, db.Table().Do(ctx,
			func(ctx context.Context, s table.Session) error {
				return nil
			},
			table.WithLabel("db.Table().Do"),
		))

		require.NoError(t, db.Table().DoTx(ctx,
			func(ctx context.Context, tx table.TransactionActor) error {
				return nil
			},
			table.WithLabel("db.Table().DoTx"),
		))

		for _, key := range []string{
			"db.Table().Do",
			"db.Table().DoTx",
		} {
			require.True(t, retryCalled[key], key)
		}
	})

	t.Run("database/sql", func(t *testing.T) {
		var (
			retryCalled = make(map[string]bool, 2)
			scope       = newScope(t)
			nativeDb    = scope.Driver(
				ydb.WithTraceRetry(trace.Retry{
					OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
						retryCalled[info.Label] = true
						return nil
					},
				}),
			)
			db = sql.OpenDB(ydb.MustConnector(nativeDb))
		)
		require.NoError(t, retry.Do(ctx, db,
			func(ctx context.Context, cc *sql.Conn) error {
				return nil
			},
			retry.WithLabel("retry.Do"),
		))

		require.NoError(t, retry.DoTx(ctx, db,
			func(ctx context.Context, tx *sql.Tx) error {
				return nil
			},
			retry.WithLabel("retry.DoTx"),
		))

		for _, key := range []string{
			"retry.Do",
			"retry.DoTx",
		} {
			require.True(t, retryCalled[key], key)
		}
	})
}
