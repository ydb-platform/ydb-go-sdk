//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestRetryableErrorWithInvalidObject(t *testing.T) {
	t.Run("retry.Do", func(t *testing.T) {
		scope := newScope(t)

		var (
			newConnCounter   = 0
			closeConnCounter = 0
		)

		db := scope.SQLDriverWithFolder(
			ydb.WithDatabaseSQLTrace(trace.DatabaseSQL{
				OnConnectorConnect: func(info trace.DatabaseSQLConnectorConnectStartInfo) func(trace.DatabaseSQLConnectorConnectDoneInfo) {
					return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
						if info.Error == nil {
							newConnCounter++
						}
					}
				},
				OnConnClose: func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
					closeConnCounter++

					return nil
				},
			}),
		)

		var id *string

		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) error {
			var ccID *string
			cc.Raw(func(driverConn any) error {
				if ider, has := driverConn.(interface{ ID() string }); has {
					v := ider.ID()
					ccID = &v
				}

				return nil
			})

			require.NotNil(t, ccID)

			if id != nil {
				require.NotEqual(t, *ccID, *id)
			}

			id = ccID

			if closeConnCounter > 0 {
				return nil
			}

			return retry.RetryableError(errors.New("test error"), xerrors.Invalid(cc))
		})

		require.NoError(t, err)
		require.Equal(t, 2, newConnCounter)
		require.Equal(t, 1, closeConnCounter)

		require.NoError(t, db.Close())
		require.Equal(t, 2, newConnCounter)
		require.Equal(t, 2, closeConnCounter)
	})
	t.Run("retry.DoTx", func(t *testing.T) {
		scope := newScope(t)

		var (
			beginCounter    = 0
			rollbackCounter = 0
			commitCounter   = 0
		)

		db := scope.SQLDriverWithFolder(
			ydb.WithDatabaseSQLTrace(trace.DatabaseSQL{
				OnConnBeginTx: func(info trace.DatabaseSQLConnBeginTxStartInfo) func(trace.DatabaseSQLConnBeginTxDoneInfo) {
					return func(info trace.DatabaseSQLConnBeginTxDoneInfo) {
						if info.Error == nil {
							beginCounter++
						}
					}
				},
				OnTxRollback: func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
					rollbackCounter++

					return nil
				},
				OnTxCommit: func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
					commitCounter++

					return nil
				},
			}),
		)

		err := retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			if rollbackCounter > 0 {
				return nil
			}

			return retry.RetryableError(errors.New("test error"), xerrors.Invalid(tx))
		})

		require.NoError(t, err)
		require.Equal(t, 2, beginCounter)
		require.Equal(t, 1, rollbackCounter)
		require.Equal(t, 1, commitCounter)

		require.NoError(t, db.Close())
	})
	t.Run("db.Table().Do", func(t *testing.T) {
		scope := newScope(t)

		var (
			newSessionCounter    = 0
			deleteSessionCounter = 0
		)

		db := scope.Driver(
			ydb.WithTraceTable(trace.Table{
				OnSessionNew: func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
					return func(info trace.TableSessionNewDoneInfo) {
						if info.Error == nil {
							newSessionCounter++
						}
					}
				},
				OnSessionDelete: func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
					deleteSessionCounter++

					return nil
				},
			}),
		)

		err := db.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			if deleteSessionCounter > 0 {
				return nil
			}

			return retry.RetryableError(errors.New("test error"), xerrors.Invalid(s))
		})

		require.NoError(t, err)
		require.Equal(t, 2, newSessionCounter)
		require.Equal(t, 1, deleteSessionCounter)

		require.NoError(t, db.Close(scope.Ctx))

		require.Equal(t, 2, newSessionCounter)
		require.Equal(t, 2, deleteSessionCounter)
	})
	t.Run("db.Table().DoTx", func(t *testing.T) {
		scope := newScope(t)

		var (
			beginCounter    = 0
			commitCounter   = 0
			rollbackCounter = 0
		)

		db := scope.Driver(
			ydb.WithTraceTable(trace.Table{
				OnTxBegin: func(info trace.TableTxBeginStartInfo) func(trace.TableTxBeginDoneInfo) {
					return func(info trace.TableTxBeginDoneInfo) {
						if info.Error == nil {
							beginCounter++
						}
					}
				},
				OnTxCommit: func(info trace.TableTxCommitStartInfo) func(trace.TableTxCommitDoneInfo) {
					commitCounter++

					return nil
				},
				OnTxRollback: func(info trace.TableTxRollbackStartInfo) func(trace.TableTxRollbackDoneInfo) {
					rollbackCounter++

					return nil
				},
			}),
		)

		err := db.Table().DoTx(scope.Ctx, func(ctx context.Context, tx table.TransactionActor) error {
			if rollbackCounter > 0 {
				return nil
			}

			return retry.RetryableError(errors.New("test error"), xerrors.Invalid(tx))
		})

		require.NoError(t, err)
		require.Equal(t, 2, beginCounter)
		require.Equal(t, 1, commitCounter)
		require.Equal(t, 1, rollbackCounter)

		require.NoError(t, db.Close(scope.Ctx))
	})
	t.Run("db.Query().Do", func(t *testing.T) {
		scope := newScope(t)

		var (
			newSessionCounter    = 0
			deleteSessionCounter = 0
		)

		db := scope.Driver(
			ydb.WithTraceQuery(trace.Query{
				OnSessionCreate: func(info trace.QuerySessionCreateStartInfo) func(trace.QuerySessionCreateDoneInfo) {
					return func(info trace.QuerySessionCreateDoneInfo) {
						if info.Error == nil {
							newSessionCounter++
						}
					}
				},
				OnSessionDelete: func(info trace.QuerySessionDeleteStartInfo) func(trace.QuerySessionDeleteDoneInfo) {
					deleteSessionCounter++

					return nil
				},
			}),
		)

		err := db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
			if deleteSessionCounter > 0 {
				return nil
			}

			return retry.RetryableError(errors.New("test error"), xerrors.Invalid(s))
		})

		require.NoError(t, err)
		require.Equal(t, 2, newSessionCounter)
		require.Equal(t, 1, deleteSessionCounter)

		require.NoError(t, db.Close(scope.Ctx))

		require.Equal(t, 2, newSessionCounter)
		require.Equal(t, 2, deleteSessionCounter)
	})
	t.Run("db.Query().DoTx", func(t *testing.T) {
		scope := newScope(t)

		var (
			beginCounter    = 0
			commitCounter   = 0
			rollbackCounter = 0
		)

		db := scope.Driver(
			ydb.WithTraceQuery(trace.Query{
				OnSessionBegin: func(info trace.QuerySessionBeginStartInfo) func(trace.QuerySessionBeginDoneInfo) {
					return func(info trace.QuerySessionBeginDoneInfo) {
						if info.Error == nil {
							beginCounter++
						}
					}
				},
				OnTxCommit: func(info trace.QueryTxCommitStartInfo) func(info trace.QueryTxCommitDoneInfo) {
					commitCounter++

					return nil
				},
				OnTxRollback: func(info trace.QueryTxRollbackStartInfo) func(trace.QueryTxRollbackDoneInfo) {
					rollbackCounter++

					return nil
				},
			}),
		)

		err := db.Query().DoTx(scope.Ctx, func(ctx context.Context, tx query.TxActor) error {
			if rollbackCounter > 0 {
				return nil
			}

			return retry.RetryableError(errors.New("test error"), xerrors.Invalid(tx))
		})

		require.NoError(t, err)
		require.Equal(t, 2, beginCounter)
		require.Equal(t, 1, commitCounter)
		require.Equal(t, 1, rollbackCounter)

		require.NoError(t, db.Close(scope.Ctx))
	})
}
