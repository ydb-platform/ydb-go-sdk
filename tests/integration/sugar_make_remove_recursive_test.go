//go:build integration
// +build integration

package integration

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestSugarMakeRemoveRecursive(t *testing.T) {
	var (
		scope  = newScope(t)
		db     = scope.Driver()
		folder = path.Join(db.Name(), t.Name())
	)

	err := sugar.MakeRecursive(scope.Ctx, db, path.Join(".sys", folder, "path", "to", "tables"))
	require.Error(t, err)

	testPrefix := path.Join(folder, "path", "to", "tables")
	err = sugar.MakeRecursive(scope.Ctx, db, testPrefix)
	require.NoError(t, err)

	// Create existed folder is ok
	err = sugar.MakeRecursive(scope.Ctx, db, testPrefix)
	require.NoError(t, err)

	tablePath := path.Join(testPrefix, "tableName")
	query := fmt.Sprintf("CREATE TABLE `%v` (id Uint64, PRIMARY KEY (id))", tablePath)
	_, err = db.Scripting().Execute(scope.Ctx, query, nil)
	require.NoError(t, err)

	if version.Gte(os.Getenv("YDB_VERSION"), "23.1") {
		tablePath = path.Join(testPrefix, "columnTableName")
		query = fmt.Sprintf(
			"CREATE TABLE `%v` (id Uint64 NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH(id) WITH (STORE = COLUMN)",
			tablePath,
		)
		_, err = db.Scripting().Execute(scope.Ctx, query, nil)
		require.NoError(t, err)
	}

	err = db.Topic().Create(scope.Ctx, path.Join(testPrefix, "topic"))
	require.NoError(t, err)

	err = sugar.MakeRecursive(scope.Ctx, db,
		path.Join(folder, "path", "to", "tables", "and", "another", "child", "directory"),
	)
	require.NoError(t, err)

	err = sugar.RemoveRecursive(scope.Ctx, db, folder)
	require.NoError(t, err)

	_, err = db.Scheme().ListDirectory(scope.Ctx, folder)
	require.Error(t, err)

	// Remove unexisted path is ok
	err = sugar.RemoveRecursive(scope.Ctx, db, folder)
	require.NoError(t, err)
}
