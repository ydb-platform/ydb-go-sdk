package table

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type issue229Struct struct{}

// UnmarshalJSON implements json.Unmarshaler
func (i *issue229Struct) UnmarshalJSON(_ []byte) error {
	return nil
}

func TestIssue229UnexpectedNullWhileParseNilJsonDocumentValue(t *testing.T) {
	// https://github.com/ydb-platform/ydb-go-sdk/issues/229
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := connect(t)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx TransactionActor) error {
		res, err := tx.Execute(ctx, `SELECT Nothing(JsonDocument?) AS r`, nil)
		require.NoError(t, err)
		require.NoError(t, res.NextResultSetErr(ctx))
		require.True(t, res.NextRow())

		var val issue229Struct
		require.NoError(t, res.Scan(&val))
		return nil
	}, WithTxSettings(TxSettings(WithSerializableReadWrite())))
	require.NoError(t, err)
}

func connect(t *testing.T) ydb.Connection {
	db, err := ydb.Open(context.Background(), os.Getenv("YDB_CONNECTION_STRING"), ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")))
	require.NoError(t, err)
	return db
}
