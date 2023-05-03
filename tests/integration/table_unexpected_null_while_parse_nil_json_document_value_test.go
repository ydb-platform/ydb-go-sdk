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
)

type issue229Struct struct{}

// UnmarshalJSON implements json.Unmarshaler
func (i *issue229Struct) UnmarshalJSON(_ []byte) error {
	return nil
}

// https://github.com/ydb-platform/ydb-go-sdk/issues/229
func TestIssue229UnexpectedNullWhileParseNilJsonDocumentValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	defer func(db *ydb.Driver) {
		// cleanup
		_ = db.Close(ctx)
	}(db)
	var val issue229Struct
	err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) (err error) {
		res, err := tx.Execute(ctx, `SELECT Nothing(JsonDocument?) AS r`, nil)
		if err != nil {
			return err
		}
		if err = res.NextResultSetErr(ctx); err != nil {
			return err
		}
		if !res.NextRow() {
			return fmt.Errorf("unexpected no rows in result set (err = %w)", res.Err())
		}
		if err = res.Scan(&val); err != nil {
			return err
		}
		return res.Err()
	}, table.WithIdempotent())
	require.NoError(t, err)
}
