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
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestIssue1234UnexpectedDecimalRepresentation(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()

	tests := []struct {
		name           string
		bts            [16]byte
		precision      uint32
		scale          uint32
		expectedFormat string
	}{
		{
			bts:            [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 250, 240, 128},
			precision:      22,
			scale:          9,
			expectedFormat: "0.050000000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := types.Decimal{
				Bytes:     tt.bts,
				Precision: tt.precision,
				Scale:     tt.scale,
			}
			var actual types.Decimal

			err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
				_, result, err := s.Execute(ctx, table.DefaultTxControl(), `
					DECLARE $value AS Decimal(22,9);
					SELECT $value;`,
					table.NewQueryParameters(
						table.ValueParam("$value", types.DecimalValue(&expected)),
					),
				)
				if err != nil {
					return err
				}
				for result.NextResultSet(ctx) {
					for result.NextRow() {
						err = result.Scan(&actual)
						if err != nil {
							return err
						}
					}
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, expected, actual)
			require.Equal(t, tt.expectedFormat, actual.String())
		})
	}
}

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
		row, err := db.Query().QueryRow(ctx,
			`SELECT Decimal('100.500', 33, 12)`,
			query.WithIdempotent(),
		)
		require.NoError(t, err)

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Equal(t, uint32(12), dst.Scale)
		require.Equal(t, uint32(33), dst.Precision)
		require.Equal(t, big.NewInt(100500000000000), dst.BigInt())
		require.Equal(t, "100.500000000000", dst.String())
		require.Equal(t, "100.5", dst.Format(true))
		require.Equal(t, "100.500000000000", dst.Format(false))
	})

	t.Run("DirectScanNegative", func(t *testing.T) {
		row, err := db.Query().QueryRow(ctx,
			`SELECT Decimal('-5.33', 22, 9)`,
			query.WithIdempotent(),
		)
		require.NoError(t, err)

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Equal(t, uint32(22), dst.Precision)
		require.Equal(t, uint32(9), dst.Scale)
		require.Equal(t, big.NewInt(-5330000000), dst.BigInt())
		require.Equal(t, "-5.330000000", dst.String())
		require.Equal(t, "-5.33", dst.Format(true))
		require.Equal(t, "-5.330000000", dst.Format(false))
	})

	t.Run("DirectScanWithOtherTypes", func(t *testing.T) {
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
		require.Equal(t, big.NewInt(10010000000), amount.BigInt())
		require.Equal(t, "10.010000000", amount.String())
		require.Equal(t, "10.01", amount.Format(true))
		require.Equal(t, "10.010000000", amount.Format(false))
	})

	t.Run("DirectScanOptional", func(t *testing.T) {
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
		require.Equal(t, big.NewInt(99990000000), dst.BigInt())
		require.Equal(t, "99.990000000", dst.String())
		require.Equal(t, "99.99", dst.Format(true))
		require.Equal(t, "99.990000000", dst.Format(false))
	})
}

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

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithQueryService(true),
	)
	require.NoError(t, err)
	defer func() {
		_ = connector.Close()
	}()

	db := sql.OpenDB(connector)

	t.Run("DirectScan", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT Decimal('100.500', 33, 12)`)

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Equal(t, uint32(33), dst.Precision)
		require.Equal(t, uint32(12), dst.Scale)
		require.Equal(t, big.NewInt(100500000000000), dst.BigInt())
		require.Equal(t, "100.500000000000", dst.String())
		require.Equal(t, "100.5", dst.Format(true))
		require.Equal(t, "100.500000000000", dst.Format(false))
	})

	t.Run("DirectScanNegative", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT Decimal('-5.33', 22, 9)`)

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Equal(t, uint32(22), dst.Precision)
		require.Equal(t, uint32(9), dst.Scale)
		require.Equal(t, big.NewInt(-5330000000), dst.BigInt())
		require.Equal(t, "-5.330000000", dst.String())
		require.Equal(t, "-5.33", dst.Format(true))
		require.Equal(t, "-5.330000000", dst.Format(false))
	})

	t.Run("DirectScanWithOtherTypes", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT 42u AS id, Decimal('10.01', 22, 9) AS amount`)

		var id uint64
		var amount types.Decimal
		err = row.Scan(&id, &amount)
		require.NoError(t, err)

		require.Equal(t, uint64(42), id)
		require.Equal(t, uint32(22), amount.Precision)
		require.Equal(t, uint32(9), amount.Scale)
		require.Equal(t, big.NewInt(10010000000), amount.BigInt())
		require.Equal(t, "10.010000000", amount.String())
		require.Equal(t, "10.01", amount.Format(true))
		require.Equal(t, "10.010000000", amount.Format(false))
	})

	t.Run("DirectScanOptional", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST(NULL AS Decimal(22, 9))`)

		var dst *types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Nil(t, dst)
	})

	t.Run("DirectScanOptionalNonNull", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT JUST(Decimal('99.99', 22, 9))`)

		var dst *types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.NotNil(t, dst)
		require.Equal(t, uint32(22), dst.Precision)
		require.Equal(t, uint32(9), dst.Scale)
		require.Equal(t, big.NewInt(99990000000), dst.BigInt())
		require.Equal(t, "99.990000000", dst.String())
		require.Equal(t, "99.99", dst.Format(true))
		require.Equal(t, "99.990000000", dst.Format(false))
	})
}

func TestQueryDecimalParam(t *testing.T) {
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
		d, err := types.DecimalValueFromString("100.5", 33, 12)
		require.NoError(t, err)
		row, err := db.Query().QueryRow(ctx, `SELECT $p`,
			query.WithParameters(ydb.ParamsBuilder().
				Param("$p").Any(d).
				Build(),
			), query.WithIdempotent(),
		)
		require.NoError(t, err)

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Equal(t, uint32(12), dst.Scale)
		require.Equal(t, uint32(33), dst.Precision)
		require.Equal(t, big.NewInt(100500000000000), dst.BigInt())
		require.Equal(t, "100.500000000000", dst.String())
		require.Equal(t, "100.5", dst.Format(true))
		require.Equal(t, "100.500000000000", dst.Format(false))
	})

	t.Run("DirectScanNegative", func(t *testing.T) {
		d, err := types.DecimalValueFromString("-5.33", 22, 9)
		require.NoError(t, err)
		row, err := db.Query().QueryRow(ctx, `SELECT $p`,
			query.WithParameters(ydb.ParamsBuilder().
				Param("$p").Any(d).
				Build(),
			), query.WithIdempotent(),
		)
		require.NoError(t, err)

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Equal(t, uint32(22), dst.Precision)
		require.Equal(t, uint32(9), dst.Scale)
		require.Equal(t, big.NewInt(-5330000000), dst.BigInt())
		require.Equal(t, "-5.330000000", dst.String())
		require.Equal(t, "-5.33", dst.Format(true))
		require.Equal(t, "-5.330000000", dst.Format(false))
	})

	t.Run("DirectScanWithOtherTypes", func(t *testing.T) {
		d, err := types.DecimalValueFromString("10.01", 22, 9)
		require.NoError(t, err)
		row, err := db.Query().QueryRow(ctx, `SELECT $p1 AS id, $p2 AS amount`,
			query.WithParameters(ydb.ParamsBuilder().
				Param("$p1").Uint64(42).
				Param("$p2").Any(d).
				Build(),
			), query.WithIdempotent(),
		)
		require.NoError(t, err)

		var id uint64
		var amount types.Decimal
		err = row.Scan(&id, &amount)
		require.NoError(t, err)
		require.Equal(t, uint64(42), id)
		require.Equal(t, uint32(22), amount.Precision)
		require.Equal(t, uint32(9), amount.Scale)
		require.Equal(t, big.NewInt(10010000000), amount.BigInt())
		require.Equal(t, "10.010000000", amount.String())
		require.Equal(t, "10.01", amount.Format(true))
		require.Equal(t, "10.010000000", amount.Format(false))
	})
}

func TestDatabaseSqlDecimalParam(t *testing.T) {
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

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithQueryService(true),
	)
	require.NoError(t, err)
	defer func() {
		_ = connector.Close()
	}()

	db := sql.OpenDB(connector)

	t.Run("DirectScan", func(t *testing.T) {
		d, err := types.DecimalValueFromString("100.5", 33, 12)
		require.NoError(t, err)
		row := db.QueryRowContext(ctx, `SELECT $p`, sql.Named("p", d))

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Equal(t, uint32(33), dst.Precision)
		require.Equal(t, uint32(12), dst.Scale)
		require.Equal(t, big.NewInt(100500000000000), dst.BigInt())
		require.Equal(t, "100.500000000000", dst.String())
		require.Equal(t, "100.5", dst.Format(true))
		require.Equal(t, "100.500000000000", dst.Format(false))
	})

	t.Run("DirectScanNegative", func(t *testing.T) {
		d, err := types.DecimalValueFromString("-5.33", 22, 9)
		require.NoError(t, err)
		row := db.QueryRowContext(ctx, `SELECT $p`, sql.Named("p", d))

		var dst types.Decimal
		err = row.Scan(&dst)
		require.NoError(t, err)
		require.Equal(t, uint32(22), dst.Precision)
		require.Equal(t, uint32(9), dst.Scale)
		require.Equal(t, big.NewInt(-5330000000), dst.BigInt())
		require.Equal(t, "-5.330000000", dst.String())
		require.Equal(t, "-5.33", dst.Format(true))
		require.Equal(t, "-5.330000000", dst.Format(false))
	})

	t.Run("DirectScanWithOtherTypes", func(t *testing.T) {
		d, err := types.DecimalValueFromString("10.01", 22, 9)
		require.NoError(t, err)
		row := db.QueryRowContext(ctx, `SELECT $p1 AS id, $p2 AS amount`, sql.Named("p1", uint64(42)), sql.Named("p2", d))

		var id uint64
		var amount types.Decimal
		err = row.Scan(&id, &amount)
		require.NoError(t, err)

		require.Equal(t, uint64(42), id)
		require.Equal(t, uint32(22), amount.Precision)
		require.Equal(t, uint32(9), amount.Scale)
		require.Equal(t, big.NewInt(10010000000), amount.BigInt())
		require.Equal(t, "10.010000000", amount.String())
		require.Equal(t, "10.01", amount.Format(true))
		require.Equal(t, "10.010000000", amount.Format(false))
	})
}
