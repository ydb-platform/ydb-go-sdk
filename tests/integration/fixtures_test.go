package integration

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type Env struct {
	Ctx context.Context
	fixenv.Env
	Require *require.Assertions
	t       testing.TB
}

func NewEnv(t *testing.T) (context.Context, *Env) {
	at := require.New(t)
	fEnv := fixenv.NewEnv(t)
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		ctxCancel()
	})
	res := &Env{
		Ctx:     ctx,
		Env:     fEnv,
		Require: at,
		t:       t,
	}
	return ctx, res
}

func (env *Env) Logf(format string, args ...interface{}) {
	env.t.Helper()
	env.t.Logf(format, args...)
}

func (env *Env) Failed() bool {
	return env.t.Failed()
}

func ConnectionString(env *Env) string {
	if envString := os.Getenv("YDB_CONNECTION_STRING"); envString != "" {
		return envString
	}
	return "grpc://localhost:2136/local"
}

func Driver(env *Env) *ydb.Driver {
	return env.CacheWithCleanup("", nil, func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
		connectionContext, cancel := context.WithTimeout(env.Ctx, time.Second*10)
		defer cancel()

		driver, err := ydb.Open(connectionContext, ConnectionString(env))
		clean := func() {
			env.Require.NoError(driver.Close(env.Ctx))
		}
		return driver, clean, err
	}).(*ydb.Driver)
}

func Folder(env *Env) string {
	return env.CacheWithCleanup(nil, nil, func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
		driver := Driver(env)
		folderPath := path.Join(driver.Name(), env.T().Name())
		env.Require.NoError(sugar.RemoveRecursive(env.Ctx, driver, folderPath))

		env.Logf("Create folder: %v", folderPath)
		env.Require.NoError(driver.Scheme().MakeDirectory(env.Ctx, folderPath))
		clean := func() {
			if !env.Failed() {
				env.Require.NoError(sugar.RemoveRecursive(env.Ctx, driver, folderPath))
			}
		}
		return folderPath, clean, nil
	}).(string)
}

// TableName return name (without path) to example table with struct:
// id Int64 NOT NULL,
// val Text
func TableName(env *Env) string {
	return env.Cache(nil, nil, func() (res interface{}, err error) {
		tableName := "table"

		err = Driver(env).Table().Do(env.Ctx, func(ctx context.Context, s table.Session) error {
			query := fmt.Sprintf(`PRAGMA TablePathPrefix("%s");

CREATE TABLE %s (
	id Int64 NOT NULL, val Text,
	PRIMARY KEY (id)
)
`, Folder(env), tableName)

			env.Logf("Create table query: %v", query)
			return s.ExecuteSchemeQuery(ctx, query)
		})
		return tableName, err
	}).(string)
}

// TablePath return path to example table with struct:
// id Int64 NOT NULL,
// val Text
func TablePath(env *Env) string {
	return path.Join(Folder(env), TableName(env))
}
