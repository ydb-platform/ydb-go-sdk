package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/framework"
	"slo/internal/generator"
	"slo/internal/kv"
)

const writeQuery = `
DECLARE $id AS Uint64;
DECLARE $payload_str AS Utf8;
DECLARE $payload_double AS Double;
DECLARE $payload_timestamp AS Timestamp;

UPSERT INTO %s (
	id, hash, payload_str, payload_double, payload_timestamp
) VALUES (
	$id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp
);
`

const readQuery = `
DECLARE $id AS Uint64;
SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
FROM %s WHERE id = $id AND hash = Digest::NumericHash($id);
`

const createTableQuery = `
CREATE TABLE IF NOT EXISTS %s (
	hash Uint64?,
	id Uint64?,
	payload_str Text?,
	payload_double Double?,
	payload_timestamp Timestamp?,
	payload_hash Uint64?,
	PRIMARY KEY (hash, id)
) WITH (
	UNIFORM_PARTITIONS = %d,
	AUTO_PARTITIONING_BY_SIZE = ENABLED,
	AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,
	AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,
	AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d
)
`

const dropTableQuery = `DROP TABLE %s`

type db struct {
	driver            *ydb.Driver
	tablePath         string // "`path`" with backticks for Query API
	readTimeout       time.Duration
	writeTimeout      time.Duration
	partitionSize     uint
	minPartitionCount uint
	maxPartitionCount uint
}

func NewStorage(ctx context.Context, fw *framework.Framework) (framework.Workload, error) {
	params := kv.ParseParams(fw, "native-query", nil)

	connectCtx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	driver, err := ydb.Open(connectCtx,
		fw.Config.Endpoint+fw.Config.Database,
		ydb.WithRetryBudget(params.RetryBudget),
		ydb.WithSessionPoolSizeLimit(params.PoolSize()),
	)
	if err != nil {
		return nil, err
	}

	return kv.New(fw, params, &db{
		driver:            driver,
		tablePath:         fmt.Sprintf("`%s`", params.TablePath),
		readTimeout:       params.ReadTimeout,
		writeTimeout:      params.WriteTimeout,
		partitionSize:     params.PartitionSize,
		minPartitionCount: params.MinPartitionCount,
		maxPartitionCount: params.MaxPartitionCount,
	}), nil
}

func (d *db) Read(ctx context.Context, id generator.RowID) (_ generator.Row, attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return generator.Row{}, 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.readTimeout)
	defer cancel()

	var row generator.Row

	err := d.driver.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			res, err := session.Query(ctx,
				fmt.Sprintf(readQuery, d.tablePath),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(id).
						Build(),
				),
				query.WithTxControl(query.TxControl(
					query.BeginTx(query.WithSnapshotReadOnly()),
					query.CommitTx(),
				)),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close(ctx)
			}()

			rs, err := res.NextResultSet(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}

				return err
			}

			r, err := rs.NextRow(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}

				return err
			}

			return r.ScanStruct(&row, query.WithScanStructAllowMissingColumnsFromSelect())
		},
		query.WithIdempotent(),
		query.WithTrace(&trace.Query{
			OnDo: func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
				return func(info trace.QueryDoDoneInfo) {
					attempts = info.Attempts
				}
			},
		}),
		query.WithLabel("READ"),
	)

	return row, attempts, err
}

func (d *db) Write(ctx context.Context, row generator.Row) (attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	err := d.driver.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx,
				fmt.Sprintf(writeQuery, d.tablePath),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(row.ID).
						Param("$payload_str").Text(*row.PayloadStr).
						Param("$payload_double").Double(*row.PayloadDouble).
						Param("$payload_timestamp").Timestamp(*row.PayloadTimestamp).
						Build(),
				),
			)
		},
		query.WithIdempotent(),
		query.WithTrace(&trace.Query{
			OnDo: func(info trace.QueryDoStartInfo) func(trace.QueryDoDoneInfo) {
				return func(info trace.QueryDoDoneInfo) {
					attempts = info.Attempts
				}
			},
		}),
		query.WithLabel("WRITE"),
	)

	return attempts, err
}

func (d *db) CreateTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.driver.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx,
				fmt.Sprintf(createTableQuery,
					d.tablePath,
					d.minPartitionCount,
					d.partitionSize,
					d.minPartitionCount,
					d.maxPartitionCount,
				),
				query.WithTxControl(query.EmptyTxControl()),
			)
		},
		query.WithIdempotent(),
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
			return session.Exec(ctx,
				fmt.Sprintf(dropTableQuery, d.tablePath),
				query.WithTxControl(query.EmptyTxControl()),
			)
		},
		query.WithIdempotent(),
		query.WithLabel("DROP TABLE"),
	)
}

func (d *db) Close(ctx context.Context) error {
	return d.driver.Close(ctx)
}
