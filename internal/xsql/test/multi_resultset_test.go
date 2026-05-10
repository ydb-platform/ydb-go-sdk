package test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

// twoStatementsSQL exercises the multi-statement contract end-to-end with
// heterogeneous result sets:
//   - first  RS: 1 column, INT32              → "id"    = 42
//   - second RS: 2 columns, UTF8 + STRING     → "hello" = "hello", "world" = "world"
//
// Tests built on top of this constant must NOT assume the same column set
// across result sets and have to switch between them via an explicit
// rows.NextResultSet call.
const twoStatementsSQL = `SELECT 42 AS id; SELECT "hello"u AS hello, "world" AS world`

// TesTwoStatementsTwoResultSets verifies that a YQL query with
// two SELECT statements produces two distinct result sets readable through
// database/sql.Rows.NextResultSet for both engine modes. Each result set has
// its own column layout, so the test scans them independently.
func TesTwoStatementsTwoResultSets(t *testing.T) {
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
			{name: "overQueryService", useQueryService: true},
			{name: "overTableService", useQueryService: false},
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

				ctx := context.Background()

				sqlRows, err := db.QueryContext(ctx, twoStatementsSQL)
				require.NoError(t, err)

				t.Cleanup(func() {
					_ = sqlRows.Close()
				})

				// First result set: SELECT 42 AS id
				require.True(t, sqlRows.NextResultSet())

				firstCols, err := sqlRows.Columns()
				require.NoError(t, err)
				require.Equal(t, []string{"id"}, firstCols)

				require.True(t, sqlRows.Next())
				var id int32
				require.NoError(t, sqlRows.Scan(&id))
				require.Equal(t, int32(42), id)
				require.False(t, sqlRows.Next())

				// Second result set: SELECT "hello"u AS hello, "world" AS world.
				// Column layout differs from the first set, so the test explicitly
				// switches via NextResultSet and re-reads Columns before Scan.
				require.True(t, sqlRows.NextResultSet())

				secondCols, err := sqlRows.Columns()
				require.NoError(t, err)
				require.Equal(t, []string{"hello", "world"}, secondCols)

				require.True(t, sqlRows.Next())
				var (
					hello string
					world []byte
				)
				require.NoError(t, sqlRows.Scan(&hello, &world))
				require.Equal(t, "hello", hello)
				require.Equal(t, []byte("world"), world)
				require.False(t, sqlRows.Next())

				require.False(t, sqlRows.NextResultSet())
				require.NoError(t, sqlRows.Err())
			})
		}
	})
}
