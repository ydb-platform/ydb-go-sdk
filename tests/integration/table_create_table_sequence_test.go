//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/ydb"
)

func TestCreateTableWithSequence(sourceTest *testing.T) {
	t := xtest.MakeSyncedTest(sourceTest)
	ctx := xtest.Context(t)

	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	tablePath := path.Join(db.Name(), "TestCreateTableWithSequence", "test_table")
	startValue := int64(1000)

	// Create table with sequence
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, tablePath,
			options.WithColumnWithSequence("id", types.NewOptional(types.Int32), options.Sequence{StartValue: startValue}),
			options.WithColumn("title", types.NewOptional(types.Text)),
			options.WithPrimaryKeyColumn("id"),
		)
	}, table.WithIdempotent())
	require.NoError(t, err)

	// Insert row without specifying id and use RETURNING to get the generated id
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		var res result.Result
		res, err = s.Execute(ctx, table.TxControl(
			table.BeginTx(table.WithSerializableReadWrite()),
			table.CommitTx(),
		), `
			PRAGMA TablePathPrefix("`+path.Dir(tablePath)+`");
			INSERT INTO `+path.Base(tablePath)+` (title) VALUES ("test title");
		`, nil)
		if err != nil {
			return err
		}

		defer func() {
			_ = res.Close()
		}()

		// Select the inserted row to verify the id
		res, err = s.Execute(ctx, table.TxControl(
			table.BeginTx(table.WithSerializableReadWrite()),
			table.CommitTx(),
		), `
			PRAGMA TablePathPrefix("`+path.Dir(tablePath)+`");
			SELECT id FROM `+path.Base(tablePath)+` WHERE title = "test title";
		`, nil)
		if err != nil {
			return err
		}

		defer func() {
			_ = res.Close()
		}()

		rs, err := res.NextResultSet(ctx)
		require.NoError(t, err)

		row, err := rs.NextRow(ctx)
		require.NoError(t, err)

		var id *int32
		err = row.ScanNamed(
			table.NamedOptional("id", &id),
		)
		require.NoError(t, err)
		require.NotNil(t, id)
		require.Equal(t, int32(startValue), *id)

		return nil
	})
	require.NoError(t, err)
}
