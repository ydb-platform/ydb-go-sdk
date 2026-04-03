package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/framework"
	"slo/internal/generator"
	"slo/internal/kv"
)

const createTableQuery = `
	CREATE TABLE IF NOT EXISTS %s (
		hash              Uint64,
		id                Uint64,
		payload_str       Utf8,
		payload_double    Double,
		payload_timestamp Timestamp,
		payload_hash      Uint64,
		PRIMARY KEY (
			hash,
			id
		)
	) WITH (
		AUTO_PARTITIONING_BY_SIZE = ENABLED,
		AUTO_PARTITIONING_BY_LOAD = ENABLED,
		AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,
		AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,
		AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d,
		UNIFORM_PARTITIONS = %d
	);
`

const dropTableQuery = `
	DROP TABLE IF EXISTS %s;
`

const writeQuery = `
	UPSERT INTO %s (
		id, hash, payload_str, payload_double, payload_timestamp
	) VALUES (
		$id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp
	);
`

const readQuery = `
	SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
	FROM %s WHERE id = $id AND hash = Digest::NumericHash($id);
`

type db struct {
	sqlDB        *sql.DB
	connector    ydb.SQLConnector
	driver       *ydb.Driver
	createSQL    string
	dropSQL      string
	upsertSQL    string
	selectSQL    string
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func NewStorage(ctx context.Context, fw *framework.Framework) (framework.Workload, error) {
	params := kv.ParseParams(fw, "database-sql-query", nil)

	connectCtx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	driver, err := ydb.Open(connectCtx,
		fw.Config.Endpoint+fw.Config.Database,
		ydb.WithRetryBudget(params.RetryBudget),
	)
	if err != nil {
		return nil, fmt.Errorf("ydb.Open error: %w", err)
	}

	connector, err := ydb.Connector(driver,
		ydb.WithAutoDeclare(),
		ydb.WithQueryService(true),
		ydb.WithTablePathPrefix(fw.Config.Label),
	)
	if err != nil {
		return nil, fmt.Errorf("ydb.Connector error: %w", err)
	}

	sqlDB := sql.OpenDB(connector)
	sqlDB.SetMaxOpenConns(params.PoolSize())
	sqlDB.SetMaxIdleConns(params.PoolSize())
	sqlDB.SetConnMaxIdleTime(time.Second)

	return kv.New(fw, params, &db{
		sqlDB:     sqlDB,
		connector: connector,
		driver:    driver,
		createSQL: fmt.Sprintf(createTableQuery, fmt.Sprintf("`%s`", params.TablePath),
			params.PartitionSize, params.MinPartitionCount, params.MaxPartitionCount, params.MinPartitionCount),
		dropSQL:      fmt.Sprintf(dropTableQuery, fmt.Sprintf("`%s`", params.TablePath)),
		upsertSQL:    fmt.Sprintf(writeQuery, fmt.Sprintf("`%s`", params.TablePath)),
		selectSQL:    fmt.Sprintf(readQuery, fmt.Sprintf("`%s`", params.TablePath)),
		readTimeout:  params.ReadTimeout,
		writeTimeout: params.WriteTimeout,
	}), nil
}

func (d *db) Read(ctx context.Context, entryID generator.RowID) (_ generator.Row, attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return generator.Row{}, 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.readTimeout)
	defer cancel()

	var res generator.Row

	err := retry.Do(ctx, d.sqlDB,
		func(ctx context.Context, cc *sql.Conn) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			row := cc.QueryRowContext(ctx, d.selectSQL,
				sql.Named("id", &entryID),
			)

			var hash *uint64

			return row.Scan(&res.ID, &res.PayloadStr, &res.PayloadDouble, &res.PayloadTimestamp, &hash)
		},
		retry.WithIdempotent(true),
		retry.WithTrace(
			&trace.Retry{
				OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
					return func(info trace.RetryLoopDoneInfo) {
						attempts = info.Attempts
					}
				},
			},
		),
	)

	return res, attempts, err
}

func (d *db) Write(ctx context.Context, e generator.Row) (attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	err := retry.Do(ctx, d.sqlDB,
		func(ctx context.Context, cc *sql.Conn) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			_, err := cc.ExecContext(ctx, d.upsertSQL,
				sql.Named("id", e.ID),
				sql.Named("payload_str", *e.PayloadStr),
				sql.Named("payload_double", *e.PayloadDouble),
				sql.Named("payload_timestamp", *e.PayloadTimestamp),
			)

			return err
		},
		retry.WithIdempotent(true),
		retry.WithTrace(
			&trace.Retry{
				OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
					return func(info trace.RetryLoopDoneInfo) {
						attempts = info.Attempts
					}
				},
			},
		),
	)

	return attempts, err
}

func (d *db) CreateTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return retry.Do(ctx, d.sqlDB,
		func(ctx context.Context, _ *sql.Conn) error {
			_, err := d.sqlDB.ExecContext(ctx, d.createSQL)

			return err
		}, retry.WithIdempotent(true),
	)
}

func (d *db) DropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return retry.Do(ctx, d.sqlDB,
		func(ctx context.Context, _ *sql.Conn) error {
			_, err := d.sqlDB.ExecContext(ctx, d.dropSQL)

			return err
		}, retry.WithIdempotent(true),
	)
}

func (d *db) Close(ctx context.Context) error {
	_ = d.sqlDB.Close()
	_ = d.connector.Close()

	return d.driver.Close(ctx)
}
