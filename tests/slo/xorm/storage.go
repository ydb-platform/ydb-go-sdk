package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"time"

	ydbSDK "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"xorm.io/xorm"
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
	db  *xorm.Engine
	cfg *config.Config
}

func NewStorage(_ context.Context, cfg *config.Config, _ int) (*Storage, error) {
	dsn := fmt.Sprintf(
		"%s%s?go_query_bind=table_path_prefix(%s),declare,numeric&go_fake_tx=scripting&go_query_mode=scripting",
		cfg.Endpoint, cfg.DB, path.Join(cfg.DB, label),
	)

	db, err := xorm.NewEngine("ydb", dsn)
	if err != nil {
		return nil, err
	}

	db.SetTableMapper(newMapper(cfg.Table, "entry"))

	db.SetLogLevel(log.LOG_DEBUG)

	s := &Storage{
		cfg: cfg,
		db:  db,
	}

	return s, nil
}

func (s *Storage) Read(ctx context.Context, id generator.RowID) (row generator.Row, attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return generator.Row{}, attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	row.ID = id

	err = retry.Do(ydbSDK.WithTxControl(ctx, readTx), s.db.DB().DB,
		func(ctx context.Context, cc *sql.Conn) (err error) {
			has, err := s.db.Context(ctx).Get(&row)
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

	err = retry.Do(ydbSDK.WithTxControl(ctx, writeTx), s.db.DB().DB,
		func(ctx context.Context, cc *sql.Conn) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			_, err = s.db.Context(ctx).Insert(row)
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

	return s.db.Context(ctx).CreateTable(generator.Row{})
}

func (s *Storage) dropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Context(ctx).DropTable(generator.Row{})
}

func (s *Storage) close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return s.db.Context(ctx).Close()
}
