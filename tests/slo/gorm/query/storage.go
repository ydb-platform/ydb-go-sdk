package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	ydb "github.com/ydb-platform/gorm-driver"
	ydbSDK "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormLogger "gorm.io/gorm/logger"

	"slo/internal/framework"
	"slo/internal/generator"
	"slo/internal/kv"
)

const optionsTemplate = `
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d,
    UNIFORM_PARTITIONS = %d
)`

type db struct {
	gormDB       *gorm.DB
	tableName    string
	tableOptions string
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func NewStorage(_ context.Context, fw *framework.Framework) (framework.Workload, error) {
	params := kv.ParseParams(fw, "gorm-query", nil)

	gormDB, err := gorm.Open(
		ydb.Open(
			fw.Config.Endpoint+fw.Config.Database,
			ydb.WithMaxOpenConns(params.PoolSize()),
			ydb.WithMaxIdleConns(params.PoolSize()),
			ydb.WithTablePathPrefix(fw.Config.Label),
		),
		&gorm.Config{
			Logger: gormLogger.Default.LogMode(gormLogger.Warn),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("gorm.Open error: %w", err)
	}

	return kv.New(fw, params, &db{
		gormDB:    gormDB,
		tableName: fw.Config.Ref,
		tableOptions: fmt.Sprintf(optionsTemplate,
			params.PartitionSize,
			params.MinPartitionCount,
			params.MaxPartitionCount,
			params.MinPartitionCount,
		),
		readTimeout:  params.ReadTimeout,
		writeTimeout: params.WriteTimeout,
	}), nil
}

func (d *db) Read(ctx context.Context, id generator.RowID) (_ generator.Row, attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return generator.Row{}, 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.readTimeout)
	defer cancel()

	rawDB, err := d.gormDB.DB()
	if err != nil {
		return generator.Row{}, 0, err
	}

	var r generator.Row
	err = retry.Do(ctx, rawDB,
		func(ctx context.Context, _ *sql.Conn) error {
			if err = ctx.Err(); err != nil {
				return err
			}

			return d.gormDB.WithContext(ctx).Scopes(scopeTable(d.tableName)).Model(&generator.Row{}).
				First(&r, "hash = ? AND id = ?",
					clause.Expr{
						SQL:  "Digest::NumericHash(?)",
						Vars: []any{id},
					},
					id,
				).Error
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

	return r, attempts, err
}

func (d *db) Write(ctx context.Context, row generator.Row) (attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	rawDB, err := d.gormDB.DB()
	if err != nil {
		return 0, err
	}

	err = retry.Do(ctx, rawDB,
		func(ctx context.Context, _ *sql.Conn) error {
			if err = ctx.Err(); err != nil {
				return err
			}

			return d.gormDB.WithContext(ctx).Scopes(scopeTable(d.tableName)).Model(&generator.Row{}).
				Create(map[string]any{
					"Hash": clause.Expr{
						SQL:  "Digest::NumericHash(?)",
						Vars: []any{row.ID},
					},
					"ID":               row.ID,
					"PayloadStr":       row.PayloadStr,
					"PayloadDouble":    row.PayloadDouble,
					"PayloadTimestamp": row.PayloadTimestamp,
				}).Error
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
	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.gormDB.WithContext(ctx).
		Set("gorm:table_options", d.tableOptions).
		Scopes(scopeTable(d.tableName)).
		AutoMigrate(&generator.Row{})
}

func (d *db) DropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.gormDB.WithContext(ctx).
		Scopes(scopeTable(d.tableName)).
		Migrator().
		DropTable(&generator.Row{})
}

func (d *db) Close(ctx context.Context) error {
	rawDB, err := d.gormDB.WithContext(ctx).DB()
	if err != nil {
		return fmt.Errorf("get db/sql driver: %w", err)
	}

	driver, err := ydbSDK.Unwrap(rawDB)
	if err != nil {
		return fmt.Errorf("unwrap ydb driver: %w", err)
	}

	if err = rawDB.Close(); err != nil {
		return fmt.Errorf("close database/sql driver: %w", err)
	}

	return driver.Close(ctx)
}

func scopeTable(tableName string) func(tx *gorm.DB) *gorm.DB {
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Table(tableName)
	}
}
