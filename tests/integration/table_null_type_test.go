//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

// https://github.com/ydb-platform/ydb-go-sdk/issues/415
func TestNullType(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) (err error) {
		res, err := tx.Execute(ctx, `SELECT NULL AS reschedule_due;`, nil)
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
		var rescheduleDue *time.Time
		err = res.ScanNamed(
			named.Optional("reschedule_due", &rescheduleDue),
		)
		if err != nil {
			return err
		}
		t.Logf("%+v\n", rescheduleDue)
		return res.Err()
	}, table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())), table.WithIdempotent())
	require.NoError(t, err)
}
