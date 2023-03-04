//go:build !fast
// +build !fast

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"os"
	"path"
	"testing"
	"time"
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

func (scope *scopeT) AuthToken() string {
	return os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
}

func (scope *scopeT) ConnectionString() string {
	if envString := os.Getenv("YDB_CONNECTION_STRING"); envString != "" {
		return envString
	}
	return "grpc://localhost:2136/local"
}

func (scope *scopeT) Driver() *ydb.Driver {
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

		logger := xtest.Logger(scope.T())

		driver, err := ydb.Open(connectionContext, connectionString,
			ydb.WithAccessTokenCredentials(token),
			ydb.WithLogger(
				trace.DetailsAll,
				ydb.WithNamespace("ydb"),
				ydb.WithWriter(logger),
				ydb.WithMinLevel(log.INFO),
			),
		)
		clean := func() {
			scope.Require.NoError(driver.Close(scope.Ctx))
		}
		return driver, clean, err
	}).(*ydb.Driver)
}

func (scope *scopeT) Failed() bool {
	return scope.t.Failed()
}

func (scope *scopeT) Folder() string {
	return scope.CacheWithCleanup(nil, nil, func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
		driver := scope.Driver()
		folderPath := path.Join(driver.Name(), scope.T().Name())
		err = sugar.RemoveRecursive(scope.Ctx, driver, folderPath)
		scope.Require.NoError(err)

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

func (scope *scopeT) Logf(format string, args ...interface{}) {
	scope.t.Helper()
	scope.t.Logf(format, args...)
}

func (scope *scopeT) SQLDriverWithFolder() *sql.DB {
	return scope.Cache(nil, nil, func() (res interface{}, err error) {
		driver := scope.Driver()
		scope.Logf("Create sql db connector")
		connector, err := ydb.Connector(driver,
			ydb.WithTablePathPrefix(scope.Folder()),
		)
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

func (scope *scopeT) T() testing.TB {
	return scope.t
}

func (scope *scopeT) TableCDCPath() string {
	return scope.Cache(nil, nil, func() (res interface{}, err error) {
		cdcName := "keys"
		query := fmt.Sprintf(`
ALTER TABLE %s 
ADD CHANGEFEED %s WITH (
	FORMAT='json',
	MODE='KEYS_ONLY'
)
`,
			scope.TablePathBackticked(),
			cdcName,
		)
		scope.Logf("Create CDC:\n%s", query)
		err = scope.Driver().Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			return s.ExecuteSchemeQuery(ctx, query)
		})
		cdcPath := path.Join(scope.TablePath(), cdcName)
		return cdcPath, err
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
// vText Text
func (scope *scopeT) TablePath() string {
	return path.Join(scope.Folder(), scope.TableName())
}

func (scope *scopeT) TablePathBackticked() string {
	return "`" + scope.TablePath() + "`"
}

func (scope *scopeT) TopicConsumer(topicPath ...string) string {
	var topic string
	switch len(topicPath) {
	case 0:
		topic = scope.TopicPath()
	case 1:
		topic = topicPath[0]
	default:
		scope.T().Fatal("TopicConsumer support none or one topicPath only")
	}

	return scope.Cache(nil, nil, func() (res interface{}, err error) {
		consumerName := "consumer"
		err = scope.Driver().Topic().Alter(
			scope.Ctx,
			topic,
			topicoptions.AlterWithAddConsumers(topictypes.Consumer{
				Name: consumerName,
			}),
		)
		return consumerName, err
	}).(string)
}

func (scope *scopeT) TopicPath() string {
	return scope.Cache(nil, nil, func() (res interface{}, err error) {
		topicPath := path.Join(scope.Folder(), "topic")
		err = scope.Driver().Topic().Create(scope.Ctx, topicPath)
		return topicPath, err
	}).(string)
}

func (scope *scopeT) TopicReader(topicPath ...string) *topicreader.Reader {
	var topic string
	switch len(topicPath) {
	case 0:
		topic = scope.TopicPath()
	case 1:
		topic = topicPath[0]
	default:
		scope.T().Fatal("TopicReader support none or one topicPath only")
	}

	return scope.CacheWithCleanup(
		topic,
		nil,
		func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
			reader, err := scope.Driver().Topic().StartReader(
				scope.TopicConsumer(topic),
				topicoptions.ReadTopic(topic),
			)
			cleanup = func() {
				if reader != nil {
					_ = reader.Close(scope.Ctx)
				}
			}
			return reader, cleanup, err
		},
	).(*topicreader.Reader)
}

func (scope *scopeT) TopicReaderSync(topicPath ...string) *topicreader.Reader {
	var topic string
	switch len(topicPath) {
	case 0:
		topic = scope.TopicPath()
	case 1:
		topic = topicPath[0]
	default:
		scope.T().Fatal("TopicReaderSync support none or one topicPath only")
	}

	return scope.CacheWithCleanup(
		nil,
		nil,
		func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
			reader, err := scope.Driver().Topic().StartReader(
				scope.TopicConsumer(topic),
				topicoptions.ReadTopic(topic),
				topicoptions.WithCommitMode(topicoptions.CommitModeSync),
			)
			cleanup = func() {
				if reader != nil {
					_ = reader.Close(scope.Ctx)
				}
			}
			return reader, cleanup, err
		},
	).(*topicreader.Reader)
}
