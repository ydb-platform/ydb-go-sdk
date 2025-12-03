//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"math/big"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// TestQueryDecimalScan tests scanning decimal values using query client with direct Scan method
func TestQueryDecimalScan(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	defer func() {
		_ = db.Close(ctx)
	}()

	t.Run("DirectScan", func(t *testing.T) {
		// Test scanning decimal using direct Scan method
		row, err := db.Query().QueryRow(ctx,
			`SELECT Decimal('123456789.987654321', 33, 12)`,
			query.WithIdempotent(),
		)
		require.NoError(t, err)

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)

		// Verify precision and scale
		require.Equal(t, uint32(33), dst.Precision)
		require.Equal(t, uint32(12), dst.Scale)

		// Verify the value
		expectedBigInt := big.NewInt(123456789987654321)
		actualBigInt := dst.BigInt()
		require.Equal(t, expectedBigInt, actualBigInt)

		// Verify String representation
		require.Equal(t, "123456789.987654321", dst.String())
	})

	t.Run("DirectScanNegative", func(t *testing.T) {
		// Test scanning negative decimal
		row, err := db.Query().QueryRow(ctx,
			`SELECT Decimal('-5.33', 22, 9)`,
			query.WithIdempotent(),
		)
		require.NoError(t, err)

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)

		// Verify precision and scale
		require.Equal(t, uint32(22), dst.Precision)
		require.Equal(t, uint32(9), dst.Scale)

		// Verify the value
		expectedBigInt := big.NewInt(-5330000000)
		actualBigInt := dst.BigInt()
		require.Equal(t, expectedBigInt, actualBigInt)
	})

	t.Run("DirectScanWithOtherTypes", func(t *testing.T) {
		// Test scanning decimal along with other types
		row, err := db.Query().QueryRow(ctx,
			`SELECT 42u AS id, Decimal('10.01', 22, 9) AS amount`,
			query.WithIdempotent(),
		)
		require.NoError(t, err)

		var id uint64
		var amount types.Decimal
		err = row.Scan(&id, &amount)
		require.NoError(t, err)

		require.Equal(t, uint64(42), id)
		require.Equal(t, uint32(22), amount.Precision)
		require.Equal(t, uint32(9), amount.Scale)

		expectedBigInt := big.NewInt(10010000000)
		actualBigInt := amount.BigInt()
		require.Equal(t, expectedBigInt, actualBigInt)
	})

	t.Run("DirectScanOptional", func(t *testing.T) {
		// Test scanning optional decimal (NULL)
		row, err := db.Query().QueryRow(ctx,
			`SELECT CAST(NULL AS Decimal(22, 9))`,
			query.WithIdempotent(),
		)
		require.NoError(t, err)

		var dst *types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Nil(t, dst)
	})

	t.Run("DirectScanOptionalNonNull", func(t *testing.T) {
		// Test scanning optional decimal (non-NULL)
		row, err := db.Query().QueryRow(ctx,
			`SELECT JUST(Decimal('99.99', 22, 9))`,
			query.WithIdempotent(),
		)
		require.NoError(t, err)

		var dst *types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.NotNil(t, dst)
		require.Equal(t, uint32(22), dst.Precision)
		require.Equal(t, uint32(9), dst.Scale)

		expectedBigInt := big.NewInt(99990000000)
		actualBigInt := dst.BigInt()
		require.Equal(t, expectedBigInt, actualBigInt)
	})
}

// TestDatabaseSqlDecimalScan tests scanning decimal values using database/sql
func TestDatabaseSqlDecimalScan(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	nativeDriver, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	// Test both with table service (WithQueryService(false)) and query service (WithQueryService(true))
	for _, tt := range []struct {
		name         string
		queryService bool
	}{
		{
			name:         "TableService",
			queryService: false,
		},
		{
			name:         "QueryService",
			queryService: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := ydb.Connector(nativeDriver,
				ydb.WithQueryService(tt.queryService),
			)
			require.NoError(t, err)
			defer func() {
				_ = connector.Close()
			}()

			db := sql.OpenDB(connector)

			t.Run("DirectScan", func(t *testing.T) {
				// Test scanning decimal using direct Scan method
				row := db.QueryRowContext(ctx, `SELECT Decimal('123456789.987654321', 33, 12)`)

				var dst types.Decimal
				err = row.Scan(&dst)
				require.NoError(t, err)

				// Verify precision and scale
				require.Equal(t, uint32(33), dst.Precision)
				require.Equal(t, uint32(12), dst.Scale)

				// Verify the value
				expectedBigInt := big.NewInt(123456789987654321)
				actualBigInt := dst.BigInt()
				require.Equal(t, expectedBigInt, actualBigInt)

				// Verify String representation
				require.Equal(t, "123456789.987654321", dst.String())
			})

			t.Run("DirectScanNegative", func(t *testing.T) {
				// Test scanning negative decimal
				row := db.QueryRowContext(ctx, `SELECT Decimal('-5.33', 22, 9)`)

				var dst types.Decimal
				err = row.Scan(&dst)
				require.NoError(t, err)

				// Verify precision and scale
				require.Equal(t, uint32(22), dst.Precision)
				require.Equal(t, uint32(9), dst.Scale)

				// Verify the value
				expectedBigInt := big.NewInt(-5330000000)
				actualBigInt := dst.BigInt()
				require.Equal(t, expectedBigInt, actualBigInt)
			})

			t.Run("DirectScanWithOtherTypes", func(t *testing.T) {
				// Test scanning decimal along with other types
				row := db.QueryRowContext(ctx, `SELECT 42u AS id, Decimal('10.01', 22, 9) AS amount`)

				var id uint64
				var amount types.Decimal
				err = row.Scan(&id, &amount)
				require.NoError(t, err)

				require.Equal(t, uint64(42), id)
				require.Equal(t, uint32(22), amount.Precision)
				require.Equal(t, uint32(9), amount.Scale)

				expectedBigInt := big.NewInt(10010000000)
				actualBigInt := amount.BigInt()
				require.Equal(t, expectedBigInt, actualBigInt)
			})

			t.Run("DirectScanOptional", func(t *testing.T) {
				// Test scanning optional decimal (NULL)
				row := db.QueryRowContext(ctx, `SELECT CAST(NULL AS Decimal(22, 9))`)

				var dst *types.Decimal
				err = row.Scan(&dst)
				require.NoError(t, err)
				require.Nil(t, dst)
			})

			t.Run("DirectScanOptionalNonNull", func(t *testing.T) {
				// Test scanning optional decimal (non-NULL)
				row := db.QueryRowContext(ctx, `SELECT JUST(Decimal('99.99', 22, 9))`)

				var dst *types.Decimal
				err = row.Scan(&dst)
				require.NoError(t, err)
				require.NotNil(t, dst)
				require.Equal(t, uint32(22), dst.Precision)
				require.Equal(t, uint32(9), dst.Scale)

				expectedBigInt := big.NewInt(99990000000)
				actualBigInt := dst.BigInt()
				require.Equal(t, expectedBigInt, actualBigInt)
			})
		})
	}
}
