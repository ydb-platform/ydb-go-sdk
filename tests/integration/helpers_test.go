//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type scopeT struct {
	Ctx context.Context
	fixenv.Env
	Require *require.Assertions
	t       *xtest.SyncedTest
}

func newScope(t *testing.T) *scopeT {
	st := xtest.MakeSyncedTest(t)
	at := require.New(st)
	fEnv := fixenv.New(st)
	ctx, ctxCancel := context.WithCancel(context.Background())
	st.Cleanup(func() {
		ctxCancel()
	})
	res := &scopeT{
		Ctx:     ctx,
		Env:     fEnv,
		Require: at,
		t:       st,
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
	return scope.driverNamed("default", opts...)
}

func (scope *scopeT) DriverWithGRPCLogging() *ydb.Driver {
	return scope.driverNamed("grpc-logged", ydb.With(config.WithGrpcOptions(
		grpc.WithChainUnaryInterceptor(xtest.NewGrpcLogger(scope.t).UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(xtest.NewGrpcLogger(scope.t).StreamClientInterceptor),
	)),
	)
}

func (scope *scopeT) driverNamed(name string, opts ...ydb.Option) *ydb.Driver {
	f := func() (*fixenv.GenericResult[*ydb.Driver], error) {
		connectionString := scope.ConnectionString()
		scope.Logf("Connect with connection string, driver name %q: %v", name, connectionString)

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
					scope.LoggerMinLevel(log.WARN),
					trace.DetailsAll,
				),
			)...,
		)
		clean := func() {
			if driver != nil {
				scope.Require.NoError(driver.Close(scope.Ctx))
			}
		}

		return fixenv.NewGenericResultWithCleanup(driver, clean), err
	}

	return fixenv.CacheResult(scope.Env, f, fixenv.CacheOptions{CacheKey: name})
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
	f := func() (*fixenv.GenericResult[string], error) {
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
		return fixenv.NewGenericResultWithCleanup(folderPath, clean), nil
	}
	return fixenv.CacheResult(scope.Env, f)
}

func (scope *scopeT) Logger() *testLogger {
	return scope.Cache(nil, nil, func() (res interface{}, err error) {
		return newLogger(scope.t), nil
	}).(*testLogger)
}

func (scope *scopeT) LoggerMinLevel(level log.Level) *testLogger {
	return scope.Cache(level, nil, func() (res interface{}, err error) {
		return newLoggerWithMinLevel(scope.t, level), nil
	}).(*testLogger)
}

func (scope *scopeT) TopicConsumerName() string {
	return "test-consumer"
}

func (scope *scopeT) TopicPath() string {
	f := func() (*fixenv.GenericResult[string], error) {
		topicName := strings.Replace(scope.T().Name(), "/", "__", -1)
		topicPath := path.Join(scope.Folder(), topicName)
		client := scope.Driver().Topic()

		cleanup := func() {
			if !scope.Failed() {
				_ = client.Drop(scope.Ctx, topicPath)
			}
		}
		cleanup()

		err := client.Create(scope.Ctx, topicPath, topicoptions.CreateWithConsumer(
			topictypes.Consumer{
				Name: scope.TopicConsumerName(),
			},
		))

		return fixenv.NewGenericResultWithCleanup(topicPath, cleanup), err
	}
	return fixenv.CacheResult(scope.Env, f)
}

func (scope *scopeT) TopicReader() *topicreader.Reader {
	f := func() (*fixenv.GenericResult[*topicreader.Reader], error) {
		reader, err := scope.Driver().Topic().StartReader(
			scope.TopicConsumerName(),
			topicoptions.ReadTopic(scope.TopicPath()),
		)
		cleanup := func() {
			if reader != nil {
				_ = reader.Close(scope.Ctx)
			}
		}
		return fixenv.NewGenericResultWithCleanup(reader, cleanup), err
	}

	return fixenv.CacheResult(scope.Env, f)
}

func (scope *scopeT) TopicWriter() *topicwriter.Writer {
	f := func() (*fixenv.GenericResult[*topicwriter.Writer], error) {
		writer, err := scope.Driver().Topic().StartWriter(
			scope.TopicPath(),
			topicoptions.WithWriterProducerID(scope.TopicWriterProducerID()),
			topicoptions.WithWriterWaitServerAck(true),
		)
		cleanup := func() {
			if writer != nil {
				_ = writer.Close(scope.Ctx)
			}
		}
		return fixenv.NewGenericResultWithCleanup(writer, cleanup), err
	}

	return fixenv.CacheResult(scope.Env, f)
}

func (scope *scopeT) TopicWriterProducerID() string {
	return "test-producer-id"
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
		if opt != nil {
			opt(&params)
		}
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

// logger for tests
type testLogger struct {
	test     *xtest.SyncedTest
	testName string
	minLevel log.Level

	m        xsync.Mutex
	closed   bool
	messages []string
}

func newLogger(t *xtest.SyncedTest) *testLogger {
	return newLoggerWithMinLevel(t, 0)
}

func newLoggerWithMinLevel(t *xtest.SyncedTest, level log.Level) *testLogger {
	logger := &testLogger{
		test:     t,
		testName: t.Name(),
		minLevel: level,
	}
	t.Cleanup(logger.flush)
	return logger
}

func (t *testLogger) Log(ctx context.Context, msg string, fields ...log.Field) {
	t.test.Helper()
	lvl := log.LevelFromContext(ctx)
	if lvl < t.minLevel {
		return
	}

	names := log.NamesFromContext(ctx)

	loggerName := strings.Join(names, ".")
	values := make(map[string]string)
	for _, field := range fields {
		values[field.Key()] = field.String()
	}
	timeString := time.Now().UTC().Format("15:04:05.999999999") // RFC3339Nano without date and timezone
	message := fmt.Sprintf("%s: %s [%s] %s: %v (%v)", t.testName, timeString, lvl, loggerName, msg, values)
	t.m.WithLock(func() {
		if t.closed {
			_, _ = fmt.Fprintf(os.Stderr, "\nFINISHED TEST %q:\n%s\n\n", t.testName, message)
		} else {
			t.messages = append(t.messages, message)
		}
	})
}

func (t *testLogger) flush() {
	t.m.WithLock(func() {
		t.test.Helper()
		t.closed = true
		message := "\n" + strings.Join(t.messages, "\n")
		t.test.Log(message)
	})
}
