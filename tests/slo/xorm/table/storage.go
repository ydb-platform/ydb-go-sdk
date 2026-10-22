package main

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"xorm.io/xorm"
	"xorm.io/xorm/core"
	"xorm.io/xorm/log"

	"slo/internal/framework"
	"slo/internal/generator"
	"slo/internal/kv"
)

const autoPartitioningEnabled = "ENABLED"

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

type mapper struct {
	tableName  string
	objectName string
}

func newMapper(tableName, objectName string) *mapper {
	return &mapper{tableName: tableName, objectName: objectName}
}

func (m mapper) Obj2Table(_ string) string { return m.tableName }
func (m mapper) Table2Obj(_ string) string { return m.objectName }

type db struct {
	xe           *xorm.Engine
	sqlDB        *sql.DB
	connector    ydb.SQLConnector
	driver       *ydb.Driver
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func NewStorage(ctx context.Context, fw *framework.Framework) (framework.Workload, error) {
	params := kv.ParseParams(fw, "xorm-table", nil)

	connectCtx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	dsn := fw.Config.Endpoint + fw.Config.Database

	driver, err := ydb.Open(connectCtx, dsn)
	if err != nil {
		return nil, fmt.Errorf("ydb.Open error: %w", err)
	}

	connector, err := ydb.Connector(driver,
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
		ydb.WithTablePathPrefix(path.Join(driver.Name(), fw.Config.Label)),
		ydb.WithFakeTx(ydb.ScriptingQueryMode),
		ydb.WithDefaultQueryMode(ydb.ScriptingQueryMode),
	)
	if err != nil {
		return nil, fmt.Errorf("ydb.Connector error: %w", err)
	}

	sqlDB := sql.OpenDB(connector)

	xe, err := xorm.NewEngineWithDB("ydb", dsn, core.FromDB(sqlDB))
	if err != nil {
		return nil, err
	}

	xe.SetMaxOpenConns(params.PoolSize())
	xe.SetMaxIdleConns(params.PoolSize())
	xe.SetConnMaxIdleTime(time.Second)
	tableName := path.Base(params.TablePath)
	xe.SetTableMapper(newMapper(tableName, tableName))
	xe.SetLogLevel(log.LOG_DEBUG)
	xe.Dialect().SetParams(map[string]string{
		"AUTO_PARTITIONING_BY_SIZE":              autoPartitioningEnabled,
		"AUTO_PARTITIONING_BY_LOAD":              autoPartitioningEnabled,
		"AUTO_PARTITIONING_PARTITION_SIZE_MB":    strconv.FormatUint(uint64(params.PartitionSize), 10),
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT": strconv.FormatUint(uint64(params.MinPartitionCount), 10),
		"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT": strconv.FormatUint(uint64(params.MaxPartitionCount), 10),
		"UNIFORM_PARTITIONS":                     strconv.FormatUint(uint64(params.MinPartitionCount), 10),
	})

	return kv.New(fw, params, &db{
		xe:           xe,
		sqlDB:        sqlDB,
		connector:    connector,
		driver:       driver,
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

	var rows []generator.Row

	err := retry.Retry(ydb.WithTxControl(ctx, readTx),
		func(ctx context.Context) error {
			rows = nil
			var err error
			rows, err = kv.ReadXORMRows(ctx, d.xe, entryID)

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

	var row generator.Row
	if err == nil {
		if len(rows) != 1 {
			err = fmt.Errorf("data mismatch: id %d returned %d rows, want 1", entryID, len(rows))
		} else {
			row = rows[0]
		}
	}

	return row, attempts, err
}

func (d *db) Write(ctx context.Context, row generator.Row) (attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	err := retry.Retry(ydb.WithTxControl(ctx, writeTx),
		func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			_, err := d.xe.Context(ctx).SetExpr("hash", fmt.Sprintf("Digest::NumericHash(%d)", row.ID)).Insert(row)

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

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return retry.Retry(ctx, func(ctx context.Context) error {
		return d.xe.Context(ctx).CreateTable(generator.Row{})
	})
}

func (d *db) DropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return retry.Retry(ctx, func(ctx context.Context) error {
		return d.xe.Context(ctx).DropTable(generator.Row{})
	})
}

func (d *db) Close(ctx context.Context) error {
	_ = d.xe.Context(ctx).Close()
	_ = d.sqlDB.Close()
	_ = d.connector.Close()

	return d.driver.Close(ctx)
}
