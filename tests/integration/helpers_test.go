//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path"
	"testing"
	"text/template"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type scopeT struct {
	Ctx context.Context
	fixenv.Env
	Require *require.Assertions
	t       testing.TB
}

func newScope(t *testing.T) *scopeT {
	at := require.New(t)
	fEnv := fixenv.NewEnv(t)
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		ctxCancel()
	})
	res := &scopeT{
		Ctx:     ctx,
		Env:     fEnv,
		Require: at,
		t:       t,
	}
	return res
}

func (scope *scopeT) T() testing.TB {
	return scope.t
}

func (scope *scopeT) Logf(format string, args ...interface{}) {
	scope.t.Helper()
	scope.t.Logf(format, args...)
}

func (scope *scopeT) Failed() bool {
	return scope.t.Failed()
}

func (scope *scopeT) ConnectionString() string {
	if envString := os.Getenv("YDB_CONNECTION_STRING"); envString != "" {
		return envString
	}
	return "grpc://localhost:2136/local"
}

func (scope *scopeT) AuthToken() string {
	return os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
}

func (scope *scopeT) Driver(opts ...ydb.Option) *ydb.Driver {
	return scope.CacheWithCleanup("", nil, func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
		connectionString := scope.ConnectionString()
		scope.Logf("Connect with connection string: %v", connectionString)

		token := scope.AuthToken()
		if token == "" {
			scope.Logf("With empty auth token")
		} else {
			scope.Logf("With auth token")
		}

		connectionContext, cancel := context.WithTimeout(scope.Ctx, time.Second*10)
		defer cancel()

		driver, err := ydb.Open(connectionContext, connectionString,
			append(opts,
				ydb.WithAccessTokenCredentials(token),
				ydb.WithLogger(
					log.Default(os.Stderr,
						log.WithMinLevel(log.WARN),
					),
					trace.DetailsAll,
				),
			)...,
		)
		clean := func() {
			scope.Require.NoError(driver.Close(scope.Ctx))
		}
		return driver, clean, err
	}).(*ydb.Driver)
}

func (scope *scopeT) SQLDriver(opts ...ydb.ConnectorOption) *sql.DB {
	return scope.Cache(nil, nil, func() (res interface{}, err error) {
		driver := scope.Driver()
		scope.Logf("Create sql db connector")
		connector, err := ydb.Connector(driver, opts...)
		if err != nil {
			return nil, err
		}

		db := sql.OpenDB(connector)

		scope.Logf("Ping db")
		err = db.PingContext(scope.Ctx)
		if err != nil {
			return nil, err
		}
		return db, nil
	}).(*sql.DB)
}

func (scope *scopeT) SQLDriverWithFolder(opts ...ydb.ConnectorOption) *sql.DB {
	return scope.SQLDriver(
		append([]ydb.ConnectorOption{ydb.WithTablePathPrefix(scope.Folder())}, opts...)...,
	)
}

func (scope *scopeT) Folder() string {
	return scope.CacheWithCleanup(nil, nil, func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
		driver := scope.Driver()
		folderPath := path.Join(driver.Name(), scope.T().Name())
		scope.Require.NoError(sugar.RemoveRecursive(scope.Ctx, driver, folderPath))

		scope.Logf("Create folder: %v", folderPath)
		scope.Require.NoError(driver.Scheme().MakeDirectory(scope.Ctx, folderPath))
		clean := func() {
			if !scope.Failed() {
				scope.Require.NoError(sugar.RemoveRecursive(scope.Ctx, driver, folderPath))
			}
		}
		return folderPath, clean, nil
	}).(string)
}

type tableNameParams struct {
	tableName                string
	createTableQueryTemplate string
	createTableOptions       []options.CreateTableOption
}

func withTableName(tableName string) func(t *tableNameParams) {
	return func(t *tableNameParams) {
		t.tableName = tableName
	}
}

func withCreateTableOptions(opts ...options.CreateTableOption) func(t *tableNameParams) {
	return func(t *tableNameParams) {
		t.createTableOptions = opts
	}
}

func withCreateTableQueryTemplate(createTableQueryTemplate string) func(t *tableNameParams) {
	return func(t *tableNameParams) {
		t.createTableQueryTemplate = createTableQueryTemplate
	}
}

// TableName return name (without path) to example table with struct:
// id Int64 NOT NULL,
// val Text
func (scope *scopeT) TableName(opts ...func(t *tableNameParams)) string {
	params := tableNameParams{
		tableName: "table",
		createTableQueryTemplate: `
			PRAGMA TablePathPrefix("{{.TablePathPrefix}}");
			CREATE TABLE {{.TableName}} (
				id Int64 NOT NULL, 
				val Text,
				PRIMARY KEY (id)
			)
		`,
	}
	for _, opt := range opts {
		opt(&params)
	}
	return scope.Cache(params.tableName, nil, func() (res interface{}, err error) {
		err = scope.Driver().Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) (err error) {
			if len(params.createTableOptions) == 0 {
				tmpl, err := template.New("").Parse(params.createTableQueryTemplate)
				if err != nil {
					return err
				}
				var query bytes.Buffer
				err = tmpl.Execute(&query, struct {
					TablePathPrefix string
					TableName       string
				}{
					TablePathPrefix: scope.Folder(),
					TableName:       params.tableName,
				})
				if err != nil {
					return err
				}
				if err != nil {
					panic(err)
				}
				return s.ExecuteSchemeQuery(ctx, query.String())
			}
			return s.CreateTable(ctx, path.Join(scope.Folder(), params.tableName), params.createTableOptions...)
		})
		return params.tableName, err
	}).(string)
}

// TablePath return path to example table with struct:
// id Int64 NOT NULL,
// val Text
func (scope *scopeT) TablePath(opts ...func(t *tableNameParams)) string {
	return path.Join(scope.Folder(), scope.TableName(opts...))
}
