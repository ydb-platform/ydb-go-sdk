//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"os"
	"path"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
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

func (scope *scopeT) SQLDriverWithFolder(opts ...ydb.ConnectorOption) *sql.DB {
	createOptions := append([]ydb.ConnectorOption{ydb.WithTablePathPrefix(scope.Folder())}, opts...)
	return scope.namedSQLDriver("with-folder", createOptions...)
}

func (scope *scopeT) namedSQLDriver(name string, opts ...ydb.ConnectorOption) *sql.DB {
	return scope.CacheWithCleanup(name, nil, func() (res interface{}, clean fixenv.FixtureCleanupFunc, err error) {
		driver := scope.Driver()
		scope.Logf("Create sql db connector")
		connector, err := ydb.Connector(driver, opts...)
		if err != nil {
			return nil, nil, err
		}

		db := sql.OpenDB(connector)

		scope.Logf("Ping db")
		err = db.PingContext(scope.Ctx)
		if err != nil {
			return nil, nil, err
		}

		clean = func() {
			if db != nil {
				_ = db.Close()
			}
		}
		return db, clean, nil
	}).(*sql.DB)
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

// TableName return name (without path) to example table with struct:
// id Int64 NOT NULL,
// val Text
func (scope *scopeT) TableName() string {
	return scope.Cache(nil, nil, func() (res interface{}, err error) {
		tableName := "table"

		err = scope.Driver().Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			query := fmt.Sprintf(`PRAGMA TablePathPrefix("%s");

CREATE TABLE %s (
	id Int64 NOT NULL, val Text,
	PRIMARY KEY (id)
)
`, scope.Folder(), tableName)

			scope.Logf("Create table query: %v", query)
			return s.ExecuteSchemeQuery(ctx, query)
		})
		return tableName, err
	}).(string)
}

// TablePath return path to example table with struct:
// id Int64 NOT NULL,
// val Text
func (scope *scopeT) TablePath() string {
	return path.Join(scope.Folder(), scope.TableName())
}

func (scope *scopeT) TopicConsumer() string {
	return "test-consumer"
}

func (scope *scopeT) TopicPath(opts ...topicoptions.CreateOption) string {
	return scope.Cache("", nil, func() (res interface{}, err error) {
		db := scope.Driver()
		topicPath := path.Join(scope.Folder(), "topic")
		initOptions := []topicoptions.CreateOption{
			topicoptions.CreateWithConsumer(
				topictypes.Consumer{Name: scope.TopicConsumer()},
			),
		}
		createOptions := append(initOptions, opts...)
		err = db.Topic().Create(scope.Ctx, topicPath, createOptions...)
		return topicPath, err
	}).(string)
}

func (scope *scopeT) TopicReader() *topicreader.Reader {
	return scope.CacheWithCleanup(nil, nil, func() (res interface{}, clean fixenv.FixtureCleanupFunc, err error) {
		reader, err := scope.Driver().Topic().StartReader(
			scope.TopicConsumer(), topicoptions.ReadTopic(scope.TopicPath()),
		)
		clean = func() {
			if reader != nil {
				_ = reader.Close(scope.Ctx)
			}
		}
		return reader, clean, err
	}).(*topicreader.Reader)
}

func (scope *scopeT) TopicWriter() *topicwriter.Writer {
	return scope.CacheWithCleanup(nil, nil, func() (res interface{}, clean fixenv.FixtureCleanupFunc, err error) {

		writer, err := scope.Driver().Topic().StartWriter(
			scope.TopicPath(),
			topicoptions.WithProducerID(scope.TopicProducerID()),
			topicoptions.WithSyncWrite(true),
		)
		clean = func() {
			if writer != nil {
				_ = writer.Close(scope.Ctx)
			}
		}
		return writer, clean, err
	}).(*topicwriter.Writer)
}

func (scope *scopeT) TopicProducerID() string {
	return "test-topic-writer-id"
}
