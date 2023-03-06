//go:build !fast
// +build !fast

package integration

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestSugarMakeRemoveRecursive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	db, err := ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
	)
	require.NoError(t, err, os.Getenv("YDB_CONNECTION_STRING"))
	defer func() { _ = db.Close(ctx) }()

	err = sugar.MakeRecursive(ctx, db, path.Join(".sys", t.Name(), "path", "to", "tables"))
	require.Error(t, err)

	err = sugar.MakeRecursive(ctx, db, path.Join(t.Name(), "path", "to", "tables"))
	require.NoError(t, err)

	_, err = db.Scripting().Execute(ctx, `
		PRAGMA TablePathPrefix("`+path.Join(db.Name(), t.Name(), "path", "to", "tables")+`");
		CREATE TABLE testTable (id Uint64, PRIMARY KEY (id));`, nil,
	)
	require.NoError(t, err)

	err = db.Topic().Create(ctx, path.Join(db.Name(), t.Name(), "path", "to", "topics", "testTopic"),
		topicoptions.CreateWithSupportedCodecs(topictypes.CodecRaw),
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: "test-consumer"}),
	)
	require.NoError(t, err)

	err = sugar.MakeRecursive(ctx, db, path.Join(t.Name(), "path", "to", "tables", "and", "another", "child", "directory"))
	require.NoError(t, err)

	err = sugar.RemoveRecursive(ctx, db, t.Name())
	require.NoError(t, err)
}
