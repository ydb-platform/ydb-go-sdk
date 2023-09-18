//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestKeyValue(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "23.3") {
		t.Skip("read rows not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	var (
		scope     = newScope(t)
		driver    = scope.Driver()
		tablePath = scope.TablePath()
		id        = int64(100500)
		value     = "test value"
	)

	// set
	err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		return s.BulkUpsert(ctx, tablePath, types.ListValue(types.StructValue(
			types.StructFieldValue("id", types.Int64Value(id)),
			types.StructFieldValue("val", types.TextValue(value)),
		)))
	})
	scope.Require.NoError(err)

	// get
	err = driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		rows, err := s.ReadRows(ctx, tablePath,
			types.ListValue(types.StructValue(
				types.StructFieldValue("id", types.Int64Value(id)),
			)),
			options.ReadColumn("val"),
		)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		if !rows.NextResultSet(ctx) {
			return fmt.Errorf("no result sets")
		}
		if !rows.NextRow() {
			return fmt.Errorf("no rows")
		}
		if rows.CurrentResultSet().RowCount() != 1 {
			return fmt.Errorf("wrong rows count (%d)", rows.CurrentResultSet().RowCount())
		}
		if rows.CurrentResultSet().ColumnCount() != 1 {
			return fmt.Errorf("wrong column count (%d)", rows.CurrentResultSet().ColumnCount())
		}
		var actualValue string
		if err := rows.ScanNamed(named.OptionalWithDefault("val", &actualValue)); err != nil {
			return err
		}
		t.Logf("%s[%d] = %q", tablePath, id, actualValue)
		return rows.Err()
	})
	scope.Require.NoError(err)
}
