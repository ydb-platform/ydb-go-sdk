//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestRegisterDsnParser(t *testing.T) {
	t.Run("native", func(t *testing.T) {
		var visited bool
		registrationID := ydb.RegisterDsnParser(func(dsn string) (opts []ydb.Option, _ error) {
			return []ydb.Option{
				func(ctx context.Context, d *ydb.Driver) error {
					visited = true

					return nil
				},
			}, nil
		})
		defer ydb.UnregisterDsnParser(registrationID)
		db, err := ydb.Open(context.Background(), os.Getenv("YDB_CONNECTION_STRING"))
		require.NoError(t, err)
		require.True(t, visited)
		defer func() {
			_ = db.Close(context.Background())
		}()
	})
	t.Run("database/sql", func(t *testing.T) {
		var visited bool
		registrationID := ydb.RegisterDsnParser(func(dsn string) (opts []ydb.Option, _ error) {
			return []ydb.Option{
				func(ctx context.Context, d *ydb.Driver) error {
					visited = true

					return nil
				},
			}, nil
		})
		defer ydb.UnregisterDsnParser(registrationID)
		db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
		require.NoError(t, err)
		require.True(t, visited)
		defer func() {
			_ = db.Close()
		}()
	})
}
