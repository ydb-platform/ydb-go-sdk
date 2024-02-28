//go:build integration
// +build integration

package integration

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryTxExecute(t *testing.T) {
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
	err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) (err error) {
		res, err := tx.Execute(ctx, "SELECT 1")
		if err != nil {
			return err
		}
		rs, err := res.NextResultSet(ctx)
		if err != nil {
			return err
		}
		row, err := rs.NextRow(ctx)
		if err != nil {
			return err
		}
		err = row.Scan(nil)
		if err != nil && !errors.Is(err, internalQuery.ErrNotImplemented) {
			return err
		}
		return res.Err()
	}, query.WithIdempotent())
	require.NoError(t, err)
}
