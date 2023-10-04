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
)

// https://github.com/ydb-platform/ydb-go-sdk/issues/415
func TestIssue415ScanError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
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
}

// https://github.com/ydb-platform/ydb-go-sdk/issues/847
func TestIssue847ScanError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		res, err := s.StreamExecuteScanQuery(ctx, `SELICT 1;`, nil)
		if err != nil {
			return err
		}
		return res.Err()
	}, table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())))
	require.Error(t, err)
	require.ErrorContains(t, err, "Unexpected token 'SELICT'")
}
