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
)

func TestTzTimestamp(t *testing.T) {
	ctx := context.Background()
	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()
	err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) (err error) {
		microseconds := int64(1680021427000000)
		res, err := tx.Execute(ctx, fmt.Sprintf(`SELECT CAST(CAST(%d AS Timestamp) AS TzTimestamp);`, microseconds), nil)
		if err != nil {
			return err
		}
		if err = res.NextResultSetErr(ctx); err != nil {
			return err
		}
		if !res.NextRow() {
			return fmt.Errorf("unexpected no rows in result set (err = %w)", res.Err())
		}
		var v *time.Time
		if err = res.Scan(&v); err != nil {
			return err
		}
		require.NotNil(t, v)
		require.Equal(t, microseconds, v.UTC().UnixMicro())
		return res.Err()
	}, table.WithIdempotent())
	require.NoError(t, err)
}
