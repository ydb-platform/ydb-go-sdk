package main

import (
	"context"
	"database/sql"
	"errors"
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

	"slo/internal/config"
	"slo/internal/generator"
)

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
	return &mapper{
		tableName:  tableName,
		objectName: objectName,
	}
}

func (m mapper) Obj2Table(_ string) string {
	return m.tableName
}

func (m mapper) Table2Obj(_ string) string {
	return m.objectName
}

type Storage struct {
	cc  *ydb.Driver
	c   ydb.SQLConnector
	db  *sql.DB
	x   *xorm.Engine
	cfg *config.Config
}

func NewStorage(ctx context.Context, cfg *config.Config, poolSize int) (_ *Storage, err error) {
	s := &Storage{
		cfg: cfg,
	}

	dsn := s.cfg.Endpoint + s.cfg.DB

	s.cc, err = ydb.Open(
		ctx,
		dsn,
	)
	if err != nil {
		return nil, fmt.Errorf("ydb.Open error: %w", err)
	}

	s.c, err = ydb.Connector(s.cc,
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
		ydb.WithTablePathPrefix(path.Join(s.cc.Name(), label)),
		ydb.WithFakeTx(ydb.ScriptingQueryMode),
		ydb.WithDefaultQueryMode(ydb.ScriptingQueryMode),
	)
	if err != nil {
		return nil, fmt.Errorf("ydb.Connector error: %w", err)
	}

	s.db = sql.OpenDB(s.c)

	s.x, err = xorm.NewEngineWithDB("ydb", dsn, core.FromDB(s.db))
	if err != nil {
		return nil, err
	}

	s.x.SetMaxOpenConns(poolSize)
	s.x.SetMaxIdleConns(poolSize)
	s.x.SetConnMaxIdleTime(time.Second)

	s.x.SetTableMapper(newMapper(cfg.Table, cfg.Table))

	s.x.SetLogLevel(log.LOG_DEBUG)

	tableParams := map[string]string{
		"AUTO_PARTITIONING_BY_SIZE":              "ENABLED",
		"AUTO_PARTITIONING_BY_LOAD":              "ENABLED",
		"AUTO_PARTITIONING_PARTITION_SIZE_MB":    strconv.FormatUint(s.cfg.PartitionSize, 10),
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT": strconv.FormatUint(s.cfg.MinPartitionsCount, 10),
		"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT": strconv.FormatUint(s.cfg.MaxPartitionsCount, 10),
		"UNIFORM_PARTITIONS":                     strconv.FormatUint(s.cfg.MinPartitionsCount, 10),
	}
	s.x.Dialect().SetParams(tableParams)

	return s, nil
}

func (s *Storage) Read(ctx context.Context, id generator.RowID) (row generator.Row, attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return generator.Row{}, attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	row.ID = id

	err = retry.Do(ydb.WithTxControl(ctx, readTx), s.x.DB().DB,
		func(ctx context.Context, _ *sql.Conn) (err error) {
			has, err := s.x.Context(ctx).Where("hash = Digest::NumericHash(?)", id).Get(&row)
			if err != nil {
				return fmt.Errorf("get entry error: %w", err)
			}
			if !has {
				return errors.New("get entry: entry not found")
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

	return row, attempts, err
}

func (s *Storage) Write(ctx context.Context, row generator.Row) (attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	err = retry.Do(ydb.WithTxControl(ctx, writeTx), s.x.DB().DB,
		func(ctx context.Context, _ *sql.Conn) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			_, err = s.x.Context(ctx).SetExpr("hash", fmt.Sprintf("Digest::NumericHash(%d)", row.ID)).Insert(row)
			return err
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

	return retry.Do(ctx, s.x.DB().DB, func(ctx context.Context, _ *sql.Conn) error {
		return s.x.Context(ctx).CreateTable(generator.Row{})
	})
}

func (s *Storage) dropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return retry.Do(ctx, s.x.DB().DB, func(ctx context.Context, _ *sql.Conn) error {
		return s.x.Context(ctx).DropTable(generator.Row{})
	})
}

func (s *Storage) close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := s.x.Context(ctx).Close(); err != nil {
		return fmt.Errorf("error close sessions pool: %w", err)
	}

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("error close database/sql driver: %w", err)
	}

	if err := s.c.Close(); err != nil {
		return fmt.Errorf("error close connector: %w", err)
	}

	if err := s.cc.Close(ctx); err != nil {
		return fmt.Errorf("error close ydb driver: %w", err)
	}

	return nil
}
