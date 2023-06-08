package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	ydb "github.com/ydb-platform/gorm-driver"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	ydbSDK "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormLogger "gorm.io/gorm/logger"

	"slo/internal/config"
	"slo/internal/generator"
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

type Storage struct {
	db           *gorm.DB
	cfg          *config.Config
	tableOptions string
}

func NewStorage(ctx context.Context, cfg *config.Config, poolSize int) (*Storage, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	s := &Storage{
		cfg: cfg,
		tableOptions: fmt.Sprintf(optionsTemplate,
			cfg.PartitionSize, cfg.MinPartitionsCount, cfg.MaxPartitionsCount, cfg.MinPartitionsCount),
	}

	var err error
	s.db, err = gorm.Open(
		ydb.Open(
			cfg.Endpoint+cfg.DB,
			ydb.With(
				environ.WithEnvironCredentials(ctx),
				ydbZap.WithTraces(
					logger,
					trace.DetailsAll,
				),
				ydbSDK.WithSessionPoolSizeLimit(poolSize),
			),
			ydb.WithMaxOpenConns(poolSize),
			ydb.WithMaxIdleConns(poolSize),
			ydb.WithTablePathPrefix(label),
		),
		&gorm.Config{
			Logger: gormLogger.Default.LogMode(gormLogger.Warn),
		},
	)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) Read(ctx context.Context, id generator.RowID) (r generator.Row, attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return generator.Row{}, attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	db, err := s.db.DB()
	if err != nil {
		return generator.Row{}, attempts, err
	}

	err = retry.Do(ydbSDK.WithTxControl(ctx, readTx), db,
		func(ctx context.Context, cc *sql.Conn) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			err = s.db.WithContext(ctx).Scopes(addTableToScope(s.cfg.Table)).Model(&generator.Row{}).
				First(&r, "hash = ? AND id = ?",
					clause.Expr{
						SQL:  "Digest::NumericHash(?)",
						Vars: []interface{}{id},
					},
					id,
				).Error
			if err != nil {
				return err
			}

			return nil
		},
		retry.WithDoRetryOptions(
			retry.WithIdempotent(true),
			retry.WithTrace(
				trace.Retry{
					OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
						return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
							return func(info trace.RetryLoopDoneInfo) {
								attempts = info.Attempts
							}
						}
					},
				},
			),
		),
	)

	return r, attempts, err
}

func (s *Storage) Write(ctx context.Context, row generator.Row) (attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	db, err := s.db.DB()
	if err != nil {
		return attempts, err
	}

	err = retry.Do(ydbSDK.WithTxControl(ctx, writeTx), db,
		func(ctx context.Context, cc *sql.Conn) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			return s.db.WithContext(ctx).Scopes(addTableToScope(s.cfg.Table)).Model(&generator.Row{}).
				Create(map[string]interface{}{
					"Hash": clause.Expr{
						SQL:  "Digest::NumericHash(?)",
						Vars: []interface{}{row.ID},
					},
					"ID":               row.ID,
					"PayloadStr":       row.PayloadStr,
					"PayloadDouble":    row.PayloadDouble,
					"PayloadTimestamp": row.PayloadTimestamp,
				}).Error
		},
		retry.WithDoRetryOptions(
			retry.WithIdempotent(true),
			retry.WithTrace(
				trace.Retry{
					OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
						return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
							return func(info trace.RetryLoopDoneInfo) {
								attempts = info.Attempts
							}
						}
					},
				},
			),
		),
	)

	return attempts, err
}

func (s *Storage) createTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.WithContext(ctx).Scopes(addTableToScope(s.cfg.Table)).
		Set("gorm:table_options", s.tableOptions).AutoMigrate(&generator.Row{})
}

func (s *Storage) dropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.WithContext(ctx).Scopes(addTableToScope(s.cfg.Table)).Migrator().DropTable(&generator.Row{})
}

func (s *Storage) close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	db, err := s.db.WithContext(ctx).DB()
	if err != nil {
		return fmt.Errorf("error get db/sql driver: %w", err)
	}

	cc, err := ydbSDK.Unwrap(db)
	if err != nil {
		return fmt.Errorf("error unwrap ydb driver: %w", err)
	}

	err = db.Close()
	if err != nil {
		return fmt.Errorf("error close database/sql driver: %w", err)
	}

	err = cc.Close(ctx)
	if err != nil {
		return fmt.Errorf("error close ydb driver: %w", err)
	}

	return nil
}

func addTableToScope(tableName string) func(tx *gorm.DB) *gorm.DB {
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Table(tableName)
	}
}
