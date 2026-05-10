package test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

// TestRowsErrAfterContextCancelOnSecondNext exercises the full
// database/sql stack (ydb.Connector → internal/xsql) for both engine modes.
//
// After reading the first row, canceling the QueryContext must stop further
// iteration; sql.Rows.Err must expose context.Canceled on the next Next for the
// Query Service path. The Table Service path materializes result rows without a
// per-row context hook in the legacy table reader, so Err may be io.EOF instead.
func TestRowsErrAfterContextCancelOnSecondNext(t *testing.T) {
	mockSrv := mock.Server(t)

	openCtx := context.Background()

	nativeDriver, err := ydb.Open(openCtx, mockSrv.ConnString(),
		ydb.WithAnonymousCredentials(),
		ydb.WithSessionPoolSizeLimit(32),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = nativeDriver.Close(openCtx)
	})

	t.Run("over", func(t *testing.T) {
		for _, tc := range []struct {
			name            string
			useQueryService bool
		}{
			{name: "QueryService", useQueryService: true},
			{name: "TableService", useQueryService: false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				connector, err := ydb.Connector(nativeDriver,
					ydb.WithQueryService(tc.useQueryService),
				)
				require.NoError(t, err)

				t.Cleanup(func() {
					_ = connector.Close()
				})

				db := sql.OpenDB(connector)

				t.Cleanup(func() {
					_ = db.Close()
				})

				ctx, cancel := context.WithCancel(context.Background())

				sqlRows, err := db.QueryContext(ctx, `SELECT 42`)
				require.NoError(t, err)

				t.Cleanup(func() {
					_ = sqlRows.Close()
				})

				require.True(t, sqlRows.Next())

				var v int64

				require.NoError(t, sqlRows.Scan(&v))
				require.Equal(t, int64(42), v)

				cancel()

				require.False(t, sqlRows.Next())

				err = sqlRows.Err()

				switch {
				case tc.useQueryService:
					require.ErrorIs(t, err, context.Canceled)
				default:
					if errors.Is(err, context.Canceled) {
						return
					}
					if err == nil || errors.Is(err, io.EOF) {
						return
					}

					t.Fatalf("unexpected Rows.Err: %v", err)
				}
			})
		}
	})
}
