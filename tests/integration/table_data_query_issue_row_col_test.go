//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"path"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestDataQueryIssueRowCol(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	exists, err := sugar.IsTableExists(ctx, db.Scheme(), path.Join(db.Name(), "users"))
	require.NoError(t, err)
	if exists {
		err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(db.Name(), "users"))
		})
		require.NoError(t, err)
	}
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		return s.CreateTable(ctx, path.Join(db.Name(), "users"),
			options.WithColumn("id", types.TypeUint64),
			options.WithPrimaryKeyColumn("id"),
		)
	})
	require.NoError(t, err)
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, _, err = s.Execute(ctx, table.DefaultTxControl(), `
				UPSERT INTO "users" (id) VALUES (1);
			`, nil,
		)
		return err
	})
	require.Error(t, err)
	err = ydb.OperationError(err)
	require.Error(t, err)
	re := regexp.MustCompile(", address = [a-zA-Z0-9.:-]+,( nodeID = \\d+,){0,1}")
	errText := re.ReplaceAllStringFunc(err.Error(), func(s string) string {
		return ","
	})
	require.Equal(t, "operation/GENERIC_ERROR (code = 400080, issues = [{2:17 => 'String literal can not be used here'}])", errText) //nolint:lll
}
