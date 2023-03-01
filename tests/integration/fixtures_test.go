//go:build !fast
// +build !fast

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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Scope struct {
	Ctx context.Context
	fixenv.Env
	Require *require.Assertions
	t       testing.TB
}

func NewScope(t *testing.T) *Scope {
	at := require.New(t)
	fEnv := fixenv.NewEnv(t)
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		ctxCancel()
	})
	res := &Scope{
		Ctx:     ctx,
		Env:     fEnv,
		Require: at,
		t:       t,
	}
	return res
}

func (env *Scope) T() testing.TB {
	return env.t
}

func (env *Scope) Logf(format string, args ...interface{}) {
	env.t.Helper()
	env.t.Logf(format, args...)
}

func (env *Scope) Failed() bool {
	return env.t.Failed()
}

func ConnectionString(env *Scope) string {
	if envString := os.Getenv("YDB_CONNECTION_STRING"); envString != "" {
		return envString
	}
	return "grpc://localhost:2136/local"
}

func AuthToken(_ *Scope) string {
	return os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
}

func Driver(env *Scope) *ydb.Driver {
	return env.CacheWithCleanup("", nil, func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
		connectionString := ConnectionString(env)
		env.Logf("Connect with connection string: %v", connectionString)

		token := AuthToken(env)
		if token == "" {
			env.Logf("With empty auth token")
		} else {
			env.Logf("With auth token")
		}

		connectionContext, cancel := context.WithTimeout(env.Ctx, time.Second*10)
		defer cancel()

		logger := xtest.Logger(env.T())

		driver, err := ydb.Open(connectionContext, connectionString,
			ydb.WithAccessTokenCredentials(token),
			ydb.WithLogger(
				trace.DetailsAll,
				ydb.WithNamespace("ydb"),
				ydb.WithOutWriter(logger),
				ydb.WithErrWriter(logger),
				ydb.WithMinLevel(log.TRACE),
			),
		)
		clean := func() {
			env.Require.NoError(driver.Close(env.Ctx))
		}
		return driver, clean, err
	}).(*ydb.Driver)
}

func Folder(env *Scope) string {
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
func TableName(env *Scope) string {
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
func TablePath(env *Scope) string {
	return path.Join(Folder(env), TableName(env))
}
