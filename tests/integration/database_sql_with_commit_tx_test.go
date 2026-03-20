//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDatabaseSqlWithCommitTxContext(t *testing.T) {
	scope := newScope(t)

	type traceCounters struct {
		queryTxCommit   atomic.Int64
		queryTxRollback atomic.Int64
		sqlTxCommit     atomic.Int64
		sqlTxRollback   atomic.Int64
	}

	newDB := func(t *testing.T, counters *traceCounters) (*sql.DB, func()) {
		t.Helper()

		driver := scope.NonCachingDriver(
			ydb.WithTraceQuery(trace.Query{
				OnTxCommit: func(info trace.QueryTxCommitStartInfo) func(trace.QueryTxCommitDoneInfo) {
					counters.queryTxCommit.Add(1)

					return func(trace.QueryTxCommitDoneInfo) {}
				},
				OnTxRollback: func(info trace.QueryTxRollbackStartInfo) func(trace.QueryTxRollbackDoneInfo) {
					counters.queryTxRollback.Add(1)

					return func(trace.QueryTxRollbackDoneInfo) {}
				},
			}),
		)

		connector, err := ydb.Connector(driver,
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
			ydb.WithQueryService(true),
			ydb.WithDatabaseSQLTrace(trace.DatabaseSQL{
				OnTxCommit: func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
					counters.sqlTxCommit.Add(1)

					return func(trace.DatabaseSQLTxCommitDoneInfo) {}
				},
				OnTxRollback: func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
					counters.sqlTxRollback.Add(1)

					return func(trace.DatabaseSQLTxRollbackDoneInfo) {}
				},
			}),
		)
		require.NoError(t, err)

		db := sql.OpenDB(connector)

		return db, func() {
			_ = db.Close()
			_ = driver.Close(scope.Ctx)
		}
	}

	t.Run("ExecWithCommitTxContext", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		_, err = tx.ExecContext(ydb.WithCommitTxContext(scope.Ctx), "SELECT 1")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		require.Equal(t, int64(1), counters.sqlTxCommit.Load())
		require.Equal(t, int64(1), counters.queryTxCommit.Load())
		require.Equal(t, int64(0), counters.sqlTxRollback.Load())
		require.Equal(t, int64(0), counters.queryTxRollback.Load())
	})

	t.Run("ExecWithCommitTxContextThenCommit", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		_, err = tx.ExecContext(scope.Ctx, "SELECT 1")
		require.NoError(t, err)

		_, err = tx.ExecContext(ydb.WithCommitTxContext(scope.Ctx), "SELECT 2")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		require.Equal(t, int64(1), counters.sqlTxCommit.Load())
		require.Equal(t, int64(1), counters.queryTxCommit.Load())
		require.Equal(t, int64(0), counters.sqlTxRollback.Load())
		require.Equal(t, int64(0), counters.queryTxRollback.Load())
	})

	t.Run("ExecWithCommitTxContextThenRollback", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		_, err = tx.ExecContext(ydb.WithCommitTxContext(scope.Ctx), "SELECT 1")
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		require.Equal(t, int64(1), counters.sqlTxRollback.Load())
		require.Equal(t, int64(0), counters.queryTxRollback.Load())
		require.Equal(t, int64(0), counters.sqlTxCommit.Load())
		require.Equal(t, int64(0), counters.queryTxCommit.Load())
	})

	t.Run("QueryWithCommitTxContext", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		rows, err := tx.QueryContext(ydb.WithCommitTxContext(scope.Ctx), "SELECT 1")
		require.NoError(t, err)

		for rows.Next() {
			var v int
			require.NoError(t, rows.Scan(&v))
			require.Equal(t, 1, v)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		err = tx.Commit()
		require.NoError(t, err)

		require.Equal(t, int64(1), counters.sqlTxCommit.Load())
		require.Equal(t, int64(1), counters.queryTxCommit.Load())
		require.Equal(t, int64(0), counters.sqlTxRollback.Load())
		require.Equal(t, int64(0), counters.queryTxRollback.Load())
	})

	t.Run("QueryWithCommitTxContextThenRollback", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		rows, err := tx.QueryContext(ydb.WithCommitTxContext(scope.Ctx), "SELECT 1")
		require.NoError(t, err)

		for rows.Next() {
			var v int
			require.NoError(t, rows.Scan(&v))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		err = tx.Rollback()
		require.NoError(t, err)

		require.Equal(t, int64(1), counters.sqlTxRollback.Load())
		require.Equal(t, int64(0), counters.queryTxRollback.Load())
		require.Equal(t, int64(0), counters.sqlTxCommit.Load())
		require.Equal(t, int64(0), counters.queryTxCommit.Load())
	})

	t.Run("QueryWithCommitTxContextCommitBeforeRowsClose", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		rows, err := tx.QueryContext(ydb.WithCommitTxContext(scope.Ctx), "SELECT 1")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		require.NoError(t, rows.Close())

		require.Equal(t, int64(1), counters.sqlTxCommit.Load())
		require.Equal(t, int64(1), counters.queryTxCommit.Load())
		require.Equal(t, int64(0), counters.sqlTxRollback.Load())
		require.Equal(t, int64(0), counters.queryTxRollback.Load())
	})

	t.Run("QueryWithCommitTxContextRollbackBeforeRowsClose", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		rows, err := tx.QueryContext(ydb.WithCommitTxContext(scope.Ctx), "SELECT 1")
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		require.NoError(t, rows.Close())

		require.Equal(t, int64(1), counters.sqlTxRollback.Load())
		require.Equal(t, int64(0), counters.queryTxRollback.Load())
		require.Equal(t, int64(0), counters.sqlTxCommit.Load())
		require.Equal(t, int64(0), counters.queryTxCommit.Load())
	})

	t.Run("CommitWithoutCommitTxContext", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		_, err = tx.ExecContext(scope.Ctx, "SELECT 1")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		require.Equal(t, int64(1), counters.sqlTxCommit.Load())
		require.Equal(t, int64(1), counters.queryTxCommit.Load())
		require.Equal(t, int64(0), counters.sqlTxRollback.Load())
		require.Equal(t, int64(0), counters.queryTxRollback.Load())
	})

	t.Run("RollbackWithoutCommitTxContext", func(t *testing.T) {
		var counters traceCounters
		db, cleanup := newDB(t, &counters)
		defer cleanup()

		tx, err := db.BeginTx(scope.Ctx, nil)
		require.NoError(t, err)

		_, err = tx.ExecContext(scope.Ctx, "SELECT 1")
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		require.Equal(t, int64(1), counters.sqlTxRollback.Load())
		require.Equal(t, int64(1), counters.queryTxRollback.Load())
		require.Equal(t, int64(0), counters.sqlTxCommit.Load())
		require.Equal(t, int64(0), counters.queryTxCommit.Load())
	})
}

func TestDatabaseSqlWithCommitTxContextExecThenRollbackDataPersisted(t *testing.T) {
	scope := newScope(t)
	tableName := scope.TableName()

	driver := scope.NonCachingDriver()

	connector, err := ydb.Connector(driver,
		ydb.WithTablePathPrefix(scope.Folder()),
		ydb.WithAutoDeclare(),
		ydb.WithQueryService(true),
	)
	require.NoError(t, err)

	db := sql.OpenDB(connector)
	defer func() {
		_ = db.Close()
		_ = driver.Close(scope.Ctx)
	}()

	_, err = db.ExecContext(scope.Ctx, fmt.Sprintf("DELETE FROM %s", tableName))
	require.NoError(t, err)

	tx, err := db.BeginTx(scope.Ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(
		ydb.WithCommitTxContext(scope.Ctx),
		fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, 'test')", tableName),
	)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	var count int
	err = db.QueryRowContext(scope.Ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestDatabaseSqlWithCommitTxContextRollbackBeforeRowsCloseDataPersisted(t *testing.T) {
	scope := newScope(t)
	tableName := scope.TableName()

	driver := scope.NonCachingDriver()

	connector, err := ydb.Connector(driver,
		ydb.WithTablePathPrefix(scope.Folder()),
		ydb.WithAutoDeclare(),
		ydb.WithQueryService(true),
	)
	require.NoError(t, err)

	db := sql.OpenDB(connector)
	defer func() {
		_ = db.Close()
		_ = driver.Close(scope.Ctx)
	}()

	_, err = db.ExecContext(scope.Ctx, fmt.Sprintf("DELETE FROM %s", tableName))
	require.NoError(t, err)

	tx, err := db.BeginTx(scope.Ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(scope.Ctx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, 'test')", tableName))
	require.NoError(t, err)

	rows, err := tx.QueryContext(
		ydb.WithCommitTxContext(scope.Ctx),
		fmt.Sprintf("SELECT id, val FROM %s", tableName),
	)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	_ = rows.Close()

	var count int
	err = db.QueryRowContext(scope.Ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestDatabaseSqlWithCommitTxContextQueryReturnsRows(t *testing.T) {
	scope := newScope(t)
	tableName := scope.TableName()

	driver := scope.NonCachingDriver()

	connector, err := ydb.Connector(driver,
		ydb.WithTablePathPrefix(scope.Folder()),
		ydb.WithAutoDeclare(),
		ydb.WithQueryService(true),
	)
	require.NoError(t, err)

	db := sql.OpenDB(connector)
	defer func() {
		_ = db.Close()
		_ = driver.Close(scope.Ctx)
	}()

	_, err = db.ExecContext(scope.Ctx, fmt.Sprintf("DELETE FROM %s", tableName))
	require.NoError(t, err)

	_, err = db.ExecContext(scope.Ctx,
		fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three')", tableName),
	)
	require.NoError(t, err)

	tx, err := db.BeginTx(scope.Ctx, nil)
	require.NoError(t, err)

	rows, err := tx.QueryContext(
		ydb.WithCommitTxContext(scope.Ctx),
		fmt.Sprintf("SELECT id, val FROM %s ORDER BY id", tableName),
	)
	require.NoError(t, err)

	type row struct {
		id  int64
		val string
	}

	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.val))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	err = tx.Commit()
	require.NoError(t, err)

	require.Equal(t, []row{
		{id: 1, val: "one"},
		{id: 2, val: "two"},
		{id: 3, val: "three"},
	}, got)
}
