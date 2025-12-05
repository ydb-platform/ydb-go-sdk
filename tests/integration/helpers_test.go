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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
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

func (scope *scopeT) Endpoint() string {
	conn := "grpc://localhost:2136/local"
	if envString := os.Getenv("YDB_CONNECTION_STRING"); envString != "" {
		conn = envString
	}
	arr := strings.Split(conn, "/")
	return arr[len(arr)-2]
}

func (scope *scopeT) AuthToken() string {
	return os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
}

func (scope *scopeT) CertFile() string {
	return os.Getenv("YDB_SSL_ROOT_CERTIFICATES_FILE")
}

func (scope *scopeT) Driver(opts ...ydb.Option) *ydb.Driver {
	return scope.driverNamed("default", opts...)
}

func (scope *scopeT) DriverWithLogs(opts ...ydb.Option) *ydb.Driver {
	return scope.driverNamed("logged",
		append(opts, ydb.WithTraceQuery(
			log.Query(
				log.Default(os.Stdout,
					log.WithLogQuery(),
					log.WithMinLevel(log.INFO),
				),
				trace.QueryEvents,
			),
		))...,
	)
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

		driver := scope.NonCachingDriver(opts...)

		clean := func() {
			if driver != nil {
				scope.Require.NoError(driver.Close(scope.Ctx))
			}
		}

		return fixenv.NewGenericResultWithCleanup(driver, clean), nil
	}

	return fixenv.CacheResult(scope.Env, f, fixenv.CacheOptions{CacheKey: name})
}

func (scope *scopeT) NonCachingDriver(opts ...ydb.Option) *ydb.Driver {
	connectionString := scope.ConnectionString()
	scope.Logf("Connect with connection string: %v", connectionString)

	token := scope.AuthToken()
	if token == "" {
		scope.Logf("With empty auth token")
		opts = append(opts, ydb.WithAnonymousCredentials())
	} else {
		scope.Logf("With auth token")
		opts = append(opts, ydb.WithAccessTokenCredentials(token))
	}
	cert := scope.CertFile()
	if cert == "" {
		scope.Logf("Without tls")
		opts = append(opts, ydb.WithTLSSInsecureSkipVerify())
	} else {
		scope.Logf("With tls")
		opts = append(opts, ydb.WithCertificatesFromFile(cert))
	}

	connectionContext, cancel := context.WithTimeout(scope.Ctx, time.Second*10)
	defer cancel()

	driver, err := ydb.Open(connectionContext, connectionString, opts...)
	scope.Require.NoError(err)

	return driver
}

func (scope *scopeT) SQLDriver(opts ...ydb.ConnectorOption) *sql.DB {
	f := func() (*fixenv.GenericResult[*sql.DB], error) {
		driver := scope.Driver()
		scope.Logf("Create database/sql connector for YDB")
		connector, err := ydb.Connector(driver, opts...)
		if err != nil {
			return nil, err
		}

		db := sql.OpenDB(connector)

		return fixenv.NewGenericResultWithCleanup(db, func() {
			scope.Require.NoError(db.Close())
		}), nil
	}
	return fixenv.CacheResult(scope.Env, f)
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

		scope.Logf("Creating folder: %v", folderPath)
		scope.Require.NoError(driver.Scheme().MakeDirectory(scope.Ctx, folderPath))
		clean := func() {
			if !scope.Failed() {
				scope.Require.NoError(sugar.RemoveRecursive(scope.Ctx, driver, folderPath))
			}
		}
		scope.Logf("Creating folder done: %v", folderPath)
		return fixenv.NewGenericResultWithCleanup(folderPath, clean), nil
	}
	return fixenv.CacheResult(scope.Env, f)
}

func (scope *scopeT) Logger() *testLogger {
	return scope.CacheResult(func() (*fixenv.Result, error) {
		return fixenv.NewResult(newLogger(scope.t)), nil
	}).(*testLogger)
}

func (scope *scopeT) LoggerMinLevel(level log.Level) *testLogger {
	return scope.CacheResult(func() (res *fixenv.Result, err error) {
		return fixenv.NewResult(newLoggerWithMinLevel(scope.t, level)), nil
	}, fixenv.CacheOptions{CacheKey: level}).(*testLogger)
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

		scope.Logf("Drop topic if exists: %q", topicPath)
		if err := client.Drop(scope.Ctx, topicPath); err != nil && !ydb.IsOperationErrorSchemeError(err) {
			scope.t.Logf("failed drop previous topic %q: %v", topicPath, err)
		}

		scope.Logf("Creating topic %q", topicPath)
		err := client.Create(scope.Ctx, topicPath, topicoptions.CreateWithConsumer(
			topictypes.Consumer{
				Name: scope.TopicConsumerName(),
			},
		))

		scope.Logf("Topic created: %q", topicPath)

		return fixenv.NewGenericResultWithCleanup(topicPath, cleanup), err
	}
	return fixenv.CacheResult(scope.Env, f)
}

func (scope *scopeT) TopicReader() *topicreader.Reader {
	return scope.TopicReaderNamed("default-reader")
}

func (scope *scopeT) TopicReaderNamed(name string) *topicreader.Reader {
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

	return fixenv.CacheResult(scope.Env, f, fixenv.CacheOptions{CacheKey: name})
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

	f := func() (*fixenv.GenericResult[string], error) {
		tablePath := path.Join(scope.Folder(), params.tableName)

		// drop previous table if exists
		err := scope.Driver().Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
			return s.DropTable(ctx, tablePath)
		})
		if err != nil && !ydb.IsOperationErrorSchemeError(err) {
			return nil, fmt.Errorf("failed to drop previous table: %w", err)
		}

		createTableErr := scope.Driver().Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) (err error) {
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

			return s.CreateTable(ctx, tablePath, params.createTableOptions...)
		})

		if createTableErr != nil {
			return nil, err
		}

		cleanup := func() {
			// doesn't drop table after fail test - for debugging
			if !scope.t.Failed() {
				_ = scope.Driver().Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
					return s.DropTable(ctx, tablePath)
				})
			}
		}

		return fixenv.NewGenericResultWithCleanup(params.tableName, cleanup), nil
	}

	return fixenv.CacheResult(scope.Env, f, fixenv.CacheOptions{CacheKey: params.tableName})
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

func driverEngine(db *sql.DB) (engine xsql.Engine) {
	cc, err := db.Conn(context.Background())
	if err != nil {
		return engine
	}

	defer func() {
		_ = cc.Close()
	}()

	cc.Raw(func(driverConn any) error {
		if ccc, has := driverConn.(interface {
			Engine() xsql.Engine
		}); has {
			engine = ccc.Engine()
		}

		return nil
	})

	return engine
}
