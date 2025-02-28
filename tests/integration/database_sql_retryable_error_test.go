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
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDatabaseSqlRetryableError(t *testing.T) {
	scope := newScope(t)

	var (
		newConnCounter   = 0
		closeConnCounter = 0
	)

	db := scope.SQLDriverWithFolder(
		ydb.WithTablePathPrefix(scope.Folder()),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
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
}
