//go:build !fast
// +build !fast

package integration

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestSugarMakeRemoveRecursive(t *testing.T) {
	var (
		scope = newScope(t)
		db    = scope.Driver()
	)

	err := sugar.MakeRecursive(scope.Ctx, db, path.Join(".sys", t.Name(), "path", "to", "tables"))
	require.Error(t, err)

	err = sugar.MakeRecursive(scope.Ctx, db, path.Join(t.Name(), "path", "to", "tables"))
	require.NoError(t, err)

	_, err = db.Scripting().Execute(scope.Ctx, `
		PRAGMA TablePathPrefix("`+path.Join(db.Name(), t.Name(), "path", "to", "tables")+`");
		CREATE TABLE testTable (id Uint64, PRIMARY KEY (id));`, nil,
	)
	require.NoError(t, err)

	err = db.Topic().Create(scope.Ctx, path.Join(db.Name(), t.Name(), "path", "to", "topics", "testTopic"),
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw),
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "test-consumer"}),
	)
	require.NoError(t, err)

	err = sugar.MakeRecursive(scope.Ctx, db,
		path.Join(t.Name(), "path", "to", "tables", "and", "another", "child", "directory"),
	)
	require.NoError(t, err)

	err = sugar.RemoveRecursive(scope.Ctx, db, t.Name())
	require.NoError(t, err)

	_, err = db.Scheme().ListDirectory(scope.Ctx, path.Join(t.Name()))
	require.Error(t, err)
}
