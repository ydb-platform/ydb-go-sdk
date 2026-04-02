package main

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/framework"
	"slo/internal/generator"
	"slo/internal/kv"
)

const writeQueryTemplate = `
PRAGMA TablePathPrefix("%s");

DECLARE $id AS Uint64;
DECLARE $payload_str AS Utf8;
DECLARE $payload_double AS Double;
DECLARE $payload_timestamp AS Timestamp;

UPSERT INTO ` + "`%s`" + ` (
	id, hash, payload_str, payload_double, payload_timestamp
) VALUES (
	$id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp
);
`

const readQueryTemplate = `
PRAGMA TablePathPrefix("%s");

DECLARE $id AS Uint64;
SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
FROM ` + "`%s`" + ` WHERE id = $id AND hash = Digest::NumericHash($id);
`

var (
	readTx = table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	writeTx = table.SerializableReadWriteTxControl(
		table.CommitTx(),
	)
)

type db struct {
	driver            *ydb.Driver
	tablePath         string // full path for CreateTable/DropTable
	writeQuery        string // pre-formatted with PRAGMA
	readQuery         string // pre-formatted with PRAGMA
	readTimeout       time.Duration
	writeTimeout      time.Duration
	partitionSize     uint
	minPartitionCount uint
	maxPartitionCount uint
}

func NewStorage(ctx context.Context, fw *framework.Framework) (framework.Workload, error) {
	params := kv.ParseParams(fw, "native-table-over-query-service", nil)

	connectCtx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	driver, err := ydb.Open(connectCtx,
		fw.Config.Endpoint+fw.Config.Database,
		ydb.WithSessionPoolSizeLimit(params.PoolSize()),
		ydb.WithRetryBudget(params.RetryBudget),
		ydb.WithExecuteDataQueryOverQueryClient(true),
	)
	if err != nil {
		return nil, err
	}

	prefix := path.Dir(params.TablePath)
	tableName := path.Base(params.TablePath)

	return kv.New(fw, params, &db{
		driver:            driver,
		tablePath:         params.TablePath,
		writeQuery:        fmt.Sprintf(writeQueryTemplate, prefix, tableName),
		readQuery:         fmt.Sprintf(readQueryTemplate, prefix, tableName),
		readTimeout:       params.ReadTimeout,
		writeTimeout:      params.WriteTimeout,
		partitionSize:     params.PartitionSize,
		minPartitionCount: params.MinPartitionCount,
		maxPartitionCount: params.MaxPartitionCount,
	}), nil
}

func (d *db) Read(ctx context.Context, id generator.RowID) (_ generator.Row, attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return generator.Row{}, 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.readTimeout)
	defer cancel()

	var row generator.Row

	err = d.driver.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			var res result.Result
			_, res, err = session.Execute(ctx, readTx, d.readQuery,
				table.NewQueryParameters(
					table.ValueParam("$id", types.Uint64Value(id)),
				),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()

			if err = res.NextResultSetErr(ctx); err != nil {
				return err
			}

			if !res.NextRow() {
				return fmt.Errorf("entry not found, id = %v", id)
			}

			err = res.ScanNamed(
				named.Required("id", &row.ID),
				named.Optional("payload_str", &row.PayloadStr),
				named.Optional("payload_double", &row.PayloadDouble),
				named.Optional("payload_timestamp", &row.PayloadTimestamp),
			)
			if err != nil {
				return err
			}

			return res.Err()
		},
		table.WithIdempotent(),
		table.WithTrace(trace.Table{
			OnDo: func(info trace.TableDoStartInfo) func(trace.TableDoDoneInfo) {
				return func(info trace.TableDoDoneInfo) {
					attempts = info.Attempts
				}
			},
		}),
	)

	return row, attempts, err
}

func (d *db) Write(ctx context.Context, row generator.Row) (attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	err := d.driver.Table().Do(ctx,
		func(ctx context.Context, session table.Session) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			_, res, err := session.Execute(ctx, writeTx, d.writeQuery,
				table.NewQueryParameters(
					table.ValueParam("$id", types.Uint64Value(row.ID)),
					table.ValueParam("$payload_str", types.UTF8Value(*row.PayloadStr)),
					table.ValueParam("$payload_double", types.DoubleValue(*row.PayloadDouble)),
					table.ValueParam("$payload_timestamp", types.TimestampValueFromTime(*row.PayloadTimestamp)),
				),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()

			return res.Err()
		},
		table.WithIdempotent(),
		table.WithTrace(trace.Table{
			OnDo: func(info trace.TableDoStartInfo) func(trace.TableDoDoneInfo) {
				return func(info trace.TableDoDoneInfo) {
					attempts = info.Attempts
				}
			},
		}),
	)

	return attempts, err
}

func (d *db) CreateTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.driver.Table().Do(ctx,
		func(ctx context.Context, session table.Session) error {
			return session.CreateTable(ctx, d.tablePath,
				options.WithColumn("hash", types.Optional(types.TypeUint64)),
				options.WithColumn("id", types.Optional(types.TypeUint64)),
				options.WithColumn("payload_str", types.Optional(types.TypeUTF8)),
				options.WithColumn("payload_double", types.Optional(types.TypeDouble)),
				options.WithColumn("payload_timestamp", types.Optional(types.TypeTimestamp)),
				options.WithColumn("payload_hash", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("hash", "id"),
				options.WithPartitioningSettings(
					options.WithPartitioningBySize(options.FeatureEnabled),
					options.WithPartitionSizeMb(uint64(d.partitionSize)),
					options.WithMinPartitionsCount(uint64(d.minPartitionCount)),
					options.WithMaxPartitionsCount(uint64(d.maxPartitionCount)),
				),
				options.WithPartitions(options.WithUniformPartitions(uint64(d.minPartitionCount))),
			)
		},
		table.WithIdempotent(),
	)
}

func (d *db) DropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.driver.Table().Do(ctx,
		func(ctx context.Context, session table.Session) error {
			return session.DropTable(ctx, d.tablePath)
		},
		table.WithIdempotent(),
	)
}

func (d *db) Close(ctx context.Context) error {
	return d.driver.Close(ctx)
}
