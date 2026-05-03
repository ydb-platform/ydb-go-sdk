package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/framework"
	"slo/internal/generator"
	"slo/internal/kv"
)

const createTableQuery = `
CREATE TABLE IF NOT EXISTS` + " `%s` " + `(
	id Uint64?,
	payload_str Text?,
	payload_double Double?,
	payload_timestamp Timestamp?,
	payload_hash Uint64?,
	PRIMARY KEY (id)
) WITH (
	UNIFORM_PARTITIONS = %d,
	AUTO_PARTITIONING_BY_SIZE = ENABLED,
	AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,
	AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,
	AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d
)
`

const dropTableQuery = "DROP TABLE IF EXISTS `%s`;"

type db struct {
	driver       *ydb.Driver
	tablePath    string
	createSQL    string
	dropSQL      string
	writeTimeout time.Duration
	readTimeout  time.Duration
}

func NewStorage(ctx context.Context, fw *framework.Framework) (framework.Workload, error) {
	var batchSize uint

	params := kv.ParseParams(fw, "bulk-upsert", func(fs *flag.FlagSet) {
		fs.UintVar(&batchSize, "batch-size", 1, "batch size for bulk operations")
	})

	connectCtx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	driver, err := ydb.Open(connectCtx,
		fw.Config.Endpoint+fw.Config.Database,
		ydb.WithSessionPoolSizeLimit(params.PoolSize()),
		ydb.WithRetryBudget(params.RetryBudget),
	)
	if err != nil {
		return nil, err
	}

	return kv.NewBatch(fw, params, batchSize, &db{
		driver:    driver,
		tablePath: params.TablePath,
		createSQL: fmt.Sprintf(createTableQuery, params.TablePath,
			params.MinPartitionCount, params.PartitionSize,
			params.MinPartitionCount, params.MaxPartitionCount,
		),
		dropSQL:      fmt.Sprintf(dropTableQuery, params.TablePath),
		writeTimeout: params.WriteTimeout,
		readTimeout:  params.ReadTimeout,
	}), nil
}

func (d *db) WriteBatch(ctx context.Context, rows []generator.Row) (attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	values := make([]types.Value, 0, len(rows))
	for _, row := range rows {
		values = append(values, types.StructValue(
			types.StructFieldValue("id", types.Uint64Value(row.ID)),
			types.StructFieldValue("payload_str", types.OptionalValue(types.TextValue(*row.PayloadStr))),
			types.StructFieldValue("payload_double", types.OptionalValue(types.DoubleValue(*row.PayloadDouble))),
			types.StructFieldValue(
				"payload_timestamp",
				types.OptionalValue(types.TimestampValue(uint64(row.PayloadTimestamp.UnixMicro()))),
			),
			types.StructFieldValue("payload_hash", types.OptionalValue(types.Uint64Value(*row.PayloadHash))),
		))
	}

	t := &trace.Retry{
		OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
			return func(info trace.RetryLoopDoneInfo) {
				attempts = info.Attempts
			}
		},
	}

	err := d.driver.Table().BulkUpsert(
		ctx,
		d.tablePath,
		table.BulkUpsertDataRows(types.ListValue(values...)),
		table.WithRetryOptions([]retry.Option{ //nolint:staticcheck
			retry.WithTrace(t),
		}),
		table.WithIdempotent(),
		table.WithLabel("WRITE"),
	)

	return attempts, err
}

func (d *db) ReadBatch(ctx context.Context, rowIDs []generator.RowID) (_ []generator.Row, attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.readTimeout)
	defer cancel()

	t := &trace.Retry{
		OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
			return func(info trace.RetryLoopDoneInfo) {
				attempts = info.Attempts
			}
		},
	}

	keys := make([]types.Value, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		keys = append(keys, types.StructValue(
			types.StructFieldValue("id", types.Uint64Value(rowID)),
		))
	}

	res, err := d.driver.Table().ReadRows(ctx, d.tablePath, types.ListValue(keys...), []options.ReadRowsOption{},
		table.WithRetryOptions([]retry.Option{ //nolint:staticcheck
			retry.WithTrace(t),
		}),
		table.WithIdempotent(),
		table.WithLabel("READ"),
	)
	if err != nil {
		return nil, attempts, err
	}
	defer func() {
		_ = res.Close()
	}()

	readRows := make([]generator.Row, 0, len(rowIDs))
	for res.NextResultSet(ctx) {
		if err = res.Err(); err != nil {
			return nil, attempts, err
		}

		if res.CurrentResultSet().Truncated() {
			return nil, attempts, fmt.Errorf("read rows result set truncated")
		}

		for res.NextRow() {
			var row generator.Row
			err = res.ScanNamed(
				named.Required("id", &row.ID),
				named.Optional("payload_str", &row.PayloadStr),
				named.Optional("payload_double", &row.PayloadDouble),
				named.Optional("payload_timestamp", &row.PayloadTimestamp),
				named.Optional("payload_hash", &row.PayloadHash),
			)
			if err != nil {
				return nil, attempts, err
			}

			readRows = append(readRows, row)
		}
	}

	return readRows, attempts, nil
}

func (d *db) CreateTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.driver.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx, d.createSQL, query.WithTxControl(query.EmptyTxControl()))
		}, query.WithIdempotent(),
		query.WithLabel("CREATE TABLE"),
	)
}

func (d *db) DropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.driver.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx, d.dropSQL, query.WithTxControl(query.EmptyTxControl()))
		},
		query.WithIdempotent(),
		query.WithLabel("DROP TABLE"),
	)
}

func (d *db) Close(ctx context.Context) error {
	return d.driver.Close(ctx)
}
