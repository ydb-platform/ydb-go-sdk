//go:build integration

package integration

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestTableListDirectory(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver()
	tablePath := path.Join(scope.Folder(), "table")

	err := db.Table().Do(scope.Ctx, func(ctx context.Context, session table.Session) error {
		return session.CreateTable(ctx, tablePath,
			options.WithColumn("id", types.TypeUint64),
			options.WithPrimaryKeyColumn("id"),
		)
	})
	require.NoError(t, err)

	directory, err := db.Scheme().ListDirectory(scope.Ctx, path.Dir(tablePath))
	require.NoError(t, err)

	tableName := path.Base(tablePath)
	for i := range directory.Children {
		entry := &directory.Children[i]
		if entry.IsTable() && entry.Name == tableName {
			return
		}
	}
	require.Failf(t, "table not found", "table %q not found in directory %q", tableName, path.Dir(tablePath))
}
