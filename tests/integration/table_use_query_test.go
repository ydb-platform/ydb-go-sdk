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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

func TestWithQueryService(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	t.Run("table.Session.Execute", func(t *testing.T) {
		var abc, def int32
		err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			_, res, err := s.Execute(ctx, table.DefaultTxControl(),
				`SELECT 123 as abc, 456 as def;`, nil,
				options.WithQueryService(),
			)
			if err != nil {
				return err
			}
			err = res.NextResultSetErr(ctx)
			if err != nil {
				return err
			}
			if !res.NextRow() {
				if err = res.Err(); err != nil {
					return err
				}
				return fmt.Errorf("unexpected empty result set")
			}
			var abc, def int32
			err = res.ScanNamed(
				named.Required("abc", &abc),
				named.Required("def", &def),
			)
			if err != nil {
				return err
			}
			t.Log(abc, def)
			return res.Err()
		}, table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())))
		require.NoError(t, err)
		require.EqualValues(t, 123, abc)
		require.EqualValues(t, 456, def)
	})
	t.Run("table.Transaction.Execute", func(t *testing.T) {
		err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) (err error) {
			res, err := tx.Execute(ctx, `SELECT 1 as abc, 2 as def;`, nil)
			if err != nil {
				return err
			}
			err = res.NextResultSetErr(ctx)
			if err != nil {
				return err
			}
			if !res.NextRow() {
				if err = res.Err(); err != nil {
					return err
				}
				return fmt.Errorf("unexpected empty result set")
			}
			var abc, def int32
			err = res.ScanNamed(
				named.Required("abc", &abc),
				named.Required("ghi", &def),
			)
			if err != nil {
				return err
			}
			t.Log(abc, def)
			return res.Err()
		}, table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())))
		require.Error(t, err)
		require.ErrorContains(t, err, "not found column 'ghi'")
	})
}
