//go:build !fast
// +build !fast

package integration

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type tableBulkUpsertScope struct {
	folder     string
	tableName  string
	batchCount int
	batchSize  int
	db         *ydb.Driver
}

type logMessage struct {
	App       string
	Host      string
	Timestamp time.Time
	HTTPCode  uint32
	Message   string
}

func (s *tableBulkUpsertScope) getLogBatch(offset int) (logs []logMessage) {
	for i := 0; i < s.batchSize; i++ {
		message := logMessage{
			App:       fmt.Sprintf("App_%d", offset/256),
			Host:      fmt.Sprintf("192.168.0.%d", offset%256),
			Timestamp: time.Now().Add(time.Millisecond * time.Duration(offset+i%1000)),
			HTTPCode:  200,
		}
		if i%2 == 0 {
			message.Message = "GET / HTTP/1.1"
		} else {
			message.Message = "GET /images/logo.png HTTP/1.1"
		}

		logs = append(logs, message)
	}
	return logs
}

func (s *tableBulkUpsertScope) createLogTable(ctx context.Context) error {
	return s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) error {
			return session.CreateTable(ctx, path.Join(s.db.Name(), s.folder, s.tableName),
				options.WithColumn("App", types.Optional(types.TypeUTF8)),
				options.WithColumn("Timestamp", types.Optional(types.TypeTimestamp)),
				options.WithColumn("Host", types.Optional(types.TypeUTF8)),
				options.WithColumn("HTTPCode", types.Optional(types.TypeUint32)),
				options.WithColumn("Message", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("App", "Timestamp", "Host"),
				options.WithProfile(
					options.WithPartitioningPolicy(
						options.WithPartitioningPolicyMode(options.PartitioningAutoSplit),
					),
				),
				options.WithPartitioningSettings(
					options.WithPartitioningByLoad(options.FeatureEnabled),
					options.WithPartitioningBySize(options.FeatureEnabled),
					options.WithMinPartitionsCount(50),
					options.WithMaxPartitionsCount(500),
				),
			)
		},
	)
}

func (s *tableBulkUpsertScope) writeLogBatch(ctx context.Context, logs []logMessage) error {
	return s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) error {
			rows := make([]types.Value, 0, len(logs))

			for _, msg := range logs {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("App", types.TextValue(msg.App)),
					types.StructFieldValue("Host", types.TextValue(msg.Host)),
					types.StructFieldValue("Timestamp", types.TimestampValueFromTime(msg.Timestamp)),
					types.StructFieldValue("HTTPCode", types.Uint32Value(msg.HTTPCode)),
					types.StructFieldValue("Message", types.TextValue(msg.Message)),
				))
			}

			return session.BulkUpsert(ctx, path.Join(s.db.Name(), s.folder, s.tableName), types.ListValue(rows...))
		},
	)
}

func TestTableBulkUpsert(t *testing.T) {
	t.Parallel()

	scope := &tableBulkUpsertScope{
		folder:     t.Name(),
		tableName:  "bulk_upsert_example",
		batchCount: 1000,
		batchSize:  1000,
	}

	var (
		ctx    = xtest.Context(t)
		logger = xtest.Logger(t)
	)

	var err error
	scope.db, err = ydb.Open(
		ctx,
		"", // corner case for check replacement of endpoint+database+secure
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithLogger(
			trace.MatchDetails(`ydb\.(driver|discovery|retry|scheme).*`),
			ydb.WithNamespace("ydb"),
			ydb.WithWriter(logger),
			ydb.WithMinLevel(log.TRACE),
		),
	)
	require.NoError(t, err)

	defer func() {
		err = scope.db.Close(ctx)
		require.NoError(t, err)
	}()

	t.Run("scheme", func(t *testing.T) {
		t.Run("prepare", func(t *testing.T) {
			err := scope.createLogTable(ctx)
			require.NoError(t, err)
		})
	})

	t.Run("data", func(t *testing.T) {
		t.Run("bulk", func(t *testing.T) {
			t.Run("upsert", func(t *testing.T) {
				for offset := 0; offset < scope.batchCount; offset++ {
					t.Run("batch#"+strconv.Itoa(offset), func(t *testing.T) {
						t.Parallel()
						var logs []logMessage
						t.Run("prepare", func(t *testing.T) {
							t.Run("data", func(t *testing.T) {
								logs = scope.getLogBatch(offset)
							})
						})
						t.Run("write", func(t *testing.T) {
							err := scope.writeLogBatch(ctx, logs)
							require.NoError(t, err)
						})
					})
				}
			})
		})
	})
}
