//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestTableTxLazy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var db *ydb.Driver

	t.Run("connect", func(t *testing.T) {
		var err error
		db, err = ydb.Open(ctx,
			"", // corner case for check replacement of endpoint+database+secure
			ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		)
		require.NoError(t, err)
	})

	t.Run("tx", func(t *testing.T) {
		t.Run("lazy", func(t *testing.T) {
			err := db.Table().Do(ctx,
				func(ctx context.Context, s table.Session) (err error) {
					// lazy open transaction on first execute query
					tx, res, err := s.Execute(ctx, table.SerializableReadWriteTxControl(), "SELECT 1", nil)
					if err != nil {
						return err // for auto-retry with driver
					}
					defer res.Close() // cleanup resources
					if err = res.Err(); err != nil {
						return err
					}
					// close transaction on last execute query
					res, err = tx.Execute(ctx, "SELECT 2", nil, options.WithCommit())
					if err != nil {
						return err
					}
					defer res.Close()
					return res.Err()
				},
				table.WithIdempotent(),
			)
			require.NoError(t, err)
		})
	})

	t.Run("disconnect", func(t *testing.T) {
		err := db.Close(ctx)
		require.NoError(t, err)
	})
}
