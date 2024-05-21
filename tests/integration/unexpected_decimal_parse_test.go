//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/decimal"
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
