//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const unusedDeclareQuery = `
	DECLARE $x AS String;
	SELECT 42;
	SELECT 43;
`

func drainSQLRows(t *testing.T, rows *sql.Rows) {
	t.Helper()
	defer func() {
		_ = rows.Close()
	}()
	for rows.NextResultSet() {
		for rows.Next() {
		}
	}
	require.NoError(t, rows.Err())
}

func TestDatabaseSQLIssuesHandler(t *testing.T) {
	scope := newScope(t)
	db := scope.SQLDriverWithFolder(ydb.WithQueryService(true))
	defer func() {
		_ = db.Close()
	}()

	t.Run("query context", func(t *testing.T) {
		var collector []*Ydb_Issue.IssueMessage
		ctx := ydb.WithIssuesHandler(scope.Ctx, func(issues []*Ydb_Issue.IssueMessage) {
			collector = append(collector, issues...)
		})
		rows, err := db.QueryContext(ctx, unusedDeclareQuery)
		require.NoError(t, err)
		drainSQLRows(t, rows)
		require.Len(t, collector, 1)
		require.Equal(t, "Symbol $x is not used", collector[0].Message)
	})

	t.Run("exec context", func(t *testing.T) {
		var collector []*Ydb_Issue.IssueMessage
		ctx := ydb.WithIssuesHandler(scope.Ctx, func(issues []*Ydb_Issue.IssueMessage) {
			collector = append(collector, issues...)
		})
		_, err := db.ExecContext(ctx, unusedDeclareQuery)
		require.NoError(t, err)
		require.Len(t, collector, 1)
		require.Equal(t, "Symbol $x is not used", collector[0].Message)
	})

	t.Run("tx query context", func(t *testing.T) {
		var collector []*Ydb_Issue.IssueMessage
		ctx := ydb.WithIssuesHandler(scope.Ctx, func(issues []*Ydb_Issue.IssueMessage) {
			collector = append(collector, issues...)
		})
		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)
		rows, err := tx.QueryContext(ctx, unusedDeclareQuery)
		require.NoError(t, err)
		drainSQLRows(t, rows)
		require.NoError(t, tx.Commit())
		require.Len(t, collector, 1)
		require.Equal(t, "Symbol $x is not used", collector[0].Message)
	})

	t.Run("tx exec context", func(t *testing.T) {
		var collector []*Ydb_Issue.IssueMessage
		ctx := ydb.WithIssuesHandler(scope.Ctx, func(issues []*Ydb_Issue.IssueMessage) {
			collector = append(collector, issues...)
		})
		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, unusedDeclareQuery)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		require.Len(t, collector, 1)
		require.Equal(t, "Symbol $x is not used", collector[0].Message)
	})

	t.Run("NoIssuesDoesNotInvokeCallback", func(t *testing.T) {
		var invoked atomic.Bool
		ctx := ydb.WithIssuesHandler(scope.Ctx, func([]*Ydb_Issue.IssueMessage) {
			invoked.Store(true)
		})
		rows, err := db.QueryContext(ctx, `SELECT 42; SELECT 43;`)
		require.NoError(t, err)
		drainSQLRows(t, rows)
		require.False(t, invoked.Load(), "callback should not run when the server returns no issues")
	})
}
