//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestTableBulkUpsert(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()
	tablePath := scope.TablePath()

	// upsert
	var rows []types.Value

	for i := int64(0); i < 10; i++ {
		val := fmt.Sprintf("value for %v", i)
		rows = append(rows, types.StructValue(
			types.StructFieldValue("id", types.Int64Value(i)),
			types.StructFieldValue("val", types.TextValue(val)),
		))
	}

	err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		return s.BulkUpsert(ctx, tablePath, types.ListValue(rows...))
	})
	scope.Require.NoError(err)
}

func assertIdValueImpl(ctx context.Context, t *testing.T, tableName string, id int64, val *string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		// ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) (err error) {
		res, err := tx.Execute(ctx, fmt.Sprintf("SELECT val FROM `%s` WHERE id = %d", tableName, id), nil)
		if err != nil {
			return err
		}
		err = res.NextResultSetErr(ctx)
		if err != nil {
			return err
		}
		require.EqualValues(t, 1, res.ResultSetCount())
		if !res.NextRow() {
			if err = res.Err(); err != nil {
				return err
			}
			return fmt.Errorf("unexpected empty result set")
		}
		var resultVal *string
		err = res.ScanNamed(
			named.Optional("val", &resultVal),
		)
		if err != nil {
			return err
		}
		if val != nil {
			require.NotEmpty(t, resultVal)
			require.EqualValues(t, *val, *resultVal)
		} else {
			require.Nil(t, resultVal)
		}

		return res.Err()
	}, table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())), table.WithIdempotent())
	require.NoError(t, err)
}

func assertIdValue(ctx context.Context, t *testing.T, tableName string, id int64, val string) {
	assertIdValueImpl(ctx, t, tableName, id, &val)
}

func assertIdValueNil(ctx context.Context, t *testing.T, tableName string, id int64) {
	assertIdValueImpl(ctx, t, tableName, id, nil)
}

func TestTableCsvBulkUpsert(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()
	tablePath := scope.TablePath()

	csv := `id,val
42,"text42"
43,"text43"`

	err := driver.Table().BulkUpsert(scope.Ctx, tablePath, table.NewBulkUpsertCsv(
		[]byte(csv),
		table.WithCsvHeader(),
	))
	scope.Require.NoError(err)

	assertIdValue(scope.Ctx, t, tablePath, 42, "text42")
	assertIdValue(scope.Ctx, t, tablePath, 43, "text43")
}

func TestTableCsvBulkUpsertDelimiter(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()
	tablePath := scope.TablePath()

	csv := `id:val
42:"text42"
43:"text43"`

	err := driver.Table().BulkUpsert(scope.Ctx, tablePath, table.NewBulkUpsertCsv(
		[]byte(csv),
		table.WithCsvHeader(),
		table.WithCsvDelimiter([]byte(":")),
	))
	scope.Require.NoError(err)

	assertIdValue(scope.Ctx, t, tablePath, 42, "text42")
	assertIdValue(scope.Ctx, t, tablePath, 43, "text43")
}

func TestTableCsvBulkUpsertNullValue(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()
	tablePath := scope.TablePath()

	csv := `id,val
42,hello
43,hello world`

	err := driver.Table().BulkUpsert(scope.Ctx, tablePath, table.NewBulkUpsertCsv(
		[]byte(csv),
		table.WithCsvHeader(),
		table.WithCsvNullValue([]byte("hello")),
	))
	scope.Require.NoError(err)

	assertIdValueNil(scope.Ctx, t, tablePath, 42)
	assertIdValue(scope.Ctx, t, tablePath, 43, "hello world")
}

func TestTableCsvBulkUpsertSkipRows(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver()
	tablePath := scope.TablePath()

	// Empty row are OK after skipped rows
	csv := `First skip row
			Second skip row

id,val
42,123
43,456

`

	err := driver.Table().BulkUpsert(scope.Ctx, tablePath, table.NewBulkUpsertCsv(
		[]byte(csv),
		table.WithCsvHeader(),
		table.WithCsvSkipRows(2),
	))
	scope.Require.NoError(err)

	assertIdValue(scope.Ctx, t, tablePath, 42, "123")
	assertIdValue(scope.Ctx, t, tablePath, 43, "456")
}
