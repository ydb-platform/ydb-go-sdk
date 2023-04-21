package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	env "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.uber.org/zap"

	"slo/internal/config"
	"slo/internal/generator"
)

const (
	createTemplate = `
CREATE TABLE %s (
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
);`
	dropTemplate   = `DROP TABLE %s;`
	upsertTemplate = `
UPSERT INTO %s (
	id, hash, payload_str, payload_double, payload_timestamp
) VALUES (
	$id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp
);
`
	selectTemplate = `
SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
FROM %s WHERE id = $id AND hash = Digest::NumericHash($id);
`
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

type Storage struct {
	cc          *ydb.Driver
	c           ydb.SQLConnector
	db          *sql.DB
	cfg         *config.Config
	createQuery string
	dropQuery   string
	upsertQuery string
	selectQuery string
}

func NewStorage(ctx context.Context, cfg *config.Config, logger *zap.Logger, poolSize int) (s *Storage, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	s = &Storage{
		cfg: cfg,
		createQuery: fmt.Sprintf(createTemplate, cfg.Table,
			cfg.PartitionSize, cfg.MinPartitionsCount, cfg.MaxPartitionsCount, cfg.MinPartitionsCount),
		dropQuery:   fmt.Sprintf(dropTemplate, cfg.Table),
		upsertQuery: fmt.Sprintf(upsertTemplate, cfg.Table),
		selectQuery: fmt.Sprintf(selectTemplate, cfg.Table),
	}

	logger.Info("queries",
		zap.String("create", s.createQuery),
		zap.String("drop", s.dropQuery),
		zap.String("upsert", s.upsertQuery),
		zap.String("select", s.selectQuery),
	)

	s.cc, err = ydb.Open(
		ctx,
		s.cfg.Endpoint+s.cfg.DB,
		env.WithEnvironCredentials(ctx),
		ydbZap.WithTraces(
			logger,
			trace.DetailsAll,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("ydb.Open error: %w", err)
	}

	s.c, err = ydb.Connector(s.cc,
		ydb.WithAutoDeclare(),
	)
	if err != nil {
		return nil, fmt.Errorf("ydb.Connector error: %w", err)
	}

	s.db = sql.OpenDB(s.c)

	s.db.SetMaxOpenConns(poolSize)
	s.db.SetMaxIdleConns(poolSize)
	s.db.SetConnMaxIdleTime(time.Second)

	return s, nil
}

func (s *Storage) Read(ctx context.Context, entryID generator.RowID) (res generator.Row, err error) {
	if err = ctx.Err(); err != nil {
		return generator.Row{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	err = retry.Do(ydb.WithTxControl(ctx, readTx), s.db,
		func(ctx context.Context, cc *sql.Conn) (err error) {
			row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.DataQueryMode), s.selectQuery,
				sql.Named("id", &entryID),
			)
			var hash uint64
			return row.Scan(&res.ID, &res.PayloadStr, &res.PayloadDouble, &res.PayloadTimestamp, &hash)
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
	)

	return res, err
}

func (s *Storage) Write(ctx context.Context, e generator.Row) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return retry.Do(ydb.WithTxControl(ctx, writeTx), s.db,
		func(ctx context.Context, cc *sql.Conn) error {
			_, err := cc.ExecContext(ydb.WithQueryMode(ctx, ydb.DataQueryMode), s.upsertQuery,
				sql.Named("id", e.ID),
				sql.Named("payload_str", *e.PayloadStr),
				sql.Named("payload_double", *e.PayloadDouble),
				sql.Named("payload_timestamp", *e.PayloadTimestamp),
			)
			return err
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
	)
}

func (s *Storage) createTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return retry.Do(ydb.WithTxControl(ctx, writeTx), s.db,
		func(ctx context.Context, cc *sql.Conn) error {
			_, err := s.db.ExecContext(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), s.createQuery)
			return err
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
	)
}

func (s *Storage) dropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return retry.Do(ydb.WithTxControl(ctx, writeTx), s.db,
		func(ctx context.Context, cc *sql.Conn) error {
			_, err := s.db.ExecContext(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), s.dropQuery)
			return err
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
	)
}

func (s *Storage) close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := s.db.Close(); err != nil {
		return err
	}
	if err := s.c.Close(); err != nil {
		return err
	}
	return s.cc.Close(ctx)
}
