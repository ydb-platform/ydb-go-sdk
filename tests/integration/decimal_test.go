//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/decimal"
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
			expected := decimal.Decimal{
				Bytes:     tt.bts,
				Precision: tt.precision,
				Scale:     tt.scale,
			}
			var actual decimal.Decimal

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

		var dst decimal.Decimal
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

		var dst decimal.Decimal
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
		var amount decimal.Decimal
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

		var dst *decimal.Decimal
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

		var dst *decimal.Decimal
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
		row, err := db.Query().QueryRow(ctx, `
			DECLARE $p AS Decimal(33,12);
			SELECT $p;
		`, query.WithParameters(ydb.ParamsBuilder().
			Param("$p").Any(d).
			Build(),
		), query.WithIdempotent())
		require.NoError(t, err)

		var dst decimal.Decimal
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
		row, err := db.Query().QueryRow(ctx, `
			DECLARE $p AS Decimal(22,9);
			SELECT $p;
		`, query.WithParameters(ydb.ParamsBuilder().
			Param("$p").Any(d).
			Build(),
		), query.WithIdempotent())
		require.NoError(t, err)

		var dst decimal.Decimal
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
		row, err := db.Query().QueryRow(ctx, `
			DECLARE $p1 AS Uint64;
			DECLARE $p2 AS Decimal(22,9);
			SELECT $p1 AS id, $p2 AS amount;
		`, query.WithParameters(ydb.ParamsBuilder().
			Param("$p1").Uint64(42).
			Param("$p2").Any(d).
			Build(),
		), query.WithIdempotent())
		require.NoError(t, err)

		var id uint64
		var amount decimal.Decimal
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

// TestIssue2018CustomDecimalScanner tests that custom decimal scanners work correctly
// with both query service and table service. This reproduces the scenario from issue #2018
// where a custom type implements sql.Scanner and needs to work with the underlying YDB value.
func TestIssue2018CustomDecimalScanner(t *testing.T) {
	// Define a custom decimal type similar to the one in issue #2018
	type MyDecimal struct {
		Bytes     [16]byte
		Precision uint32
		Scale     uint32
	}

	// Implement Scan method that needs to work with types.Value
	scanFunc := func(dst *MyDecimal) func(x interface{}) error {
		return func(x interface{}) error {
			// This is the critical part from issue #2018:
			// We need to check if x is a types.Value to use types.ToDecimal
			v, ok := x.(types.Value)
			if ok {
				d, err := types.ToDecimal(v)
				if err != nil {
					return err
				}
				if d == nil {
					return fmt.Errorf("nil decimal from types.Value")
				}
				dst.Bytes = d.Bytes
				dst.Precision = d.Precision
				dst.Scale = d.Scale
				return nil
			}

			// Fallback: check if it's already a decimal.Decimal
			dt, ok := x.(decimal.Decimal)
			if !ok {
				return fmt.Errorf("cannot cast %T to decimal", x)
			}

			dst.Bytes = dt.Bytes
			dst.Precision = dt.Precision
			dst.Scale = dt.Scale
			return nil
		}
	}

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

	// Test with both query service enabled and disabled
	for _, useQueryService := range []bool{true, false} {
		t.Run(fmt.Sprintf("WithQueryService=%v", useQueryService), func(t *testing.T) {
			connector, err := ydb.Connector(nativeDriver,
				ydb.WithQueryService(useQueryService),
			)
			require.NoError(t, err)
			defer func() {
				_ = connector.Close()
			}()

			db := sql.OpenDB(connector)

			t.Run("CustomScannerWithCast", func(t *testing.T) {
				row := db.QueryRowContext(ctx, `SELECT CAST("123.456" AS Decimal(22,9))`)

				var result decimal.Decimal
				err = row.Scan(&result)
				require.NoError(t, err)

				// Now simulate the custom scanner behavior
				var myDecimal MyDecimal
				err = scanFunc(&myDecimal)(result)
				require.NoError(t, err)

				require.Equal(t, uint32(22), myDecimal.Precision)
				require.Equal(t, uint32(9), myDecimal.Scale)
				require.Equal(t, big.NewInt(123456000000), decimal.FromBytes(myDecimal.Bytes[:], myDecimal.Precision))
			})

			t.Run("CustomScannerWithDirectValue", func(t *testing.T) {
				row := db.QueryRowContext(ctx, `SELECT Decimal("99.99", 22, 9)`)

				var result decimal.Decimal
				err = row.Scan(&result)
				require.NoError(t, err)

				var myDecimal MyDecimal
				err = scanFunc(&myDecimal)(result)
				require.NoError(t, err)

				require.Equal(t, uint32(22), myDecimal.Precision)
				require.Equal(t, uint32(9), myDecimal.Scale)
				require.Equal(t, big.NewInt(99990000000), decimal.FromBytes(myDecimal.Bytes[:], myDecimal.Precision))
			})

			t.Run("CustomScannerWithNegativeValue", func(t *testing.T) {
				row := db.QueryRowContext(ctx, `SELECT Decimal("-5.33", 22, 9)`)

				var result decimal.Decimal
				err = row.Scan(&result)
				require.NoError(t, err)

				var myDecimal MyDecimal
				err = scanFunc(&myDecimal)(result)
				require.NoError(t, err)

				require.Equal(t, uint32(22), myDecimal.Precision)
				require.Equal(t, uint32(9), myDecimal.Scale)
				require.Equal(t, big.NewInt(-5330000000), decimal.FromBytes(myDecimal.Bytes[:], myDecimal.Precision))
			})

			t.Run("CustomScannerWithOptionalValue", func(t *testing.T) {
				row := db.QueryRowContext(ctx, `SELECT JUST(Decimal("42.01", 22, 9))`)

				var result *decimal.Decimal
				err = row.Scan(&result)
				require.NoError(t, err)
				require.NotNil(t, result)

				var myDecimal MyDecimal
				err = scanFunc(&myDecimal)(*result)
				require.NoError(t, err)

				require.Equal(t, uint32(22), myDecimal.Precision)
				require.Equal(t, uint32(9), myDecimal.Scale)
				require.Equal(t, big.NewInt(42010000000), decimal.FromBytes(myDecimal.Bytes[:], myDecimal.Precision))
			})
		})
	}
}

func TestDatabaseSqlDecimal(t *testing.T) {
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

	for _, useQueryService := range []bool{true, false} {
		t.Run(fmt.Sprintf("WithQueryService=%v", useQueryService), func(t *testing.T) {
			connector, err := ydb.Connector(nativeDriver,
				ydb.WithQueryService(useQueryService),
			)

			require.NoError(t, err)
			defer func() {
				_ = connector.Close()
			}()

			db := sql.OpenDB(connector)

			t.Run("Param", func(t *testing.T) {
				t.Run("DirectScan", func(t *testing.T) {
					d, err := types.DecimalValueFromString("100.5", 33, 12)
					require.NoError(t, err)
					row := db.QueryRowContext(ctx, `
						DECLARE $p AS Decimal(33,12);
						SELECT $p;
					`, sql.Named("p", d))

					var dst decimal.Decimal
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
					row := db.QueryRowContext(ctx, `
						DECLARE $p AS Decimal(22,9);
						SELECT $p;
					`, sql.Named("p", d))

					var dst decimal.Decimal
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
					row := db.QueryRowContext(ctx, `
						DECLARE $p1 AS Uint64;
						DECLARE $p2 AS Decimal(22,9);
						SELECT $p1 AS id, $p2 AS amount;
					`, sql.Named("p1", uint64(42)), sql.Named("p2", d))

					var id uint64
					var amount decimal.Decimal
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
			})

			t.Run("Scan", func(t *testing.T) {
				t.Run("DirectScan", func(t *testing.T) {
					row := db.QueryRowContext(ctx, `SELECT Decimal('100.500', 33, 12)`)

					var dst decimal.Decimal
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

					var dst decimal.Decimal
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
					var amount decimal.Decimal
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

					var dst *decimal.Decimal
					err = row.Scan(&dst)
					require.NoError(t, err)
					require.Nil(t, dst)
				})

				t.Run("DirectScanOptionalNonNull", func(t *testing.T) {
					row := db.QueryRowContext(ctx, `SELECT JUST(Decimal('99.99', 22, 9))`)

					var dst *decimal.Decimal
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
			})
		})
	}
}
