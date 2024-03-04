package main

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/config"
	"slo/internal/generator"
)

//nolint:goconst
const (
	upsertTemplate = `
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
	selectTemplate = `
PRAGMA TablePathPrefix("%s");

DECLARE $id AS Uint64;
SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
FROM ` + "`%s`" + ` WHERE id = $id AND hash = Digest::NumericHash($id);
`
)

var (
	readTx = query.TxControl(
		query.BeginTx(
			query.WithOnlineReadOnly(),
		),
		query.CommitTx(),
	)

	writeTx = query.SerializableReadWriteTxControl(
		query.CommitTx(),
	)
)

type Storage struct {
	db          *ydb.Driver
	cfg         *config.Config
	prefix      string
	upsertQuery string
	selectQuery string
}

func NewStorage(ctx context.Context, cfg *config.Config, poolSize int) (*Storage, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	db, err := ydb.Open(
		ctx,
		cfg.Endpoint+cfg.DB,
		ydb.WithSessionPoolSizeLimit(poolSize),
	)
	if err != nil {
		return nil, err
	}

	prefix := path.Join(db.Name(), label)

	s := &Storage{
		db:          db,
		cfg:         cfg,
		prefix:      prefix,
		upsertQuery: fmt.Sprintf(upsertTemplate, prefix, cfg.Table),
		selectQuery: fmt.Sprintf(selectTemplate, prefix, cfg.Table),
	}

	return s, nil
}

func (s *Storage) Read(ctx context.Context, entryID generator.RowID) (generator.Row, int, error) {
	var attempts int
	if err := ctx.Err(); err != nil {
		return generator.Row{}, attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	e := generator.Row{}

	err := s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			_, res, err := session.Execute(ctx, s.selectQuery,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(entryID).
						Build(),
				),
				query.WithTxControl(readTx),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close(ctx)
			}()

			rs, err := res.NextResultSet(ctx)
			if err != nil {
				return err
			}

			row, err := rs.NextRow(ctx)
			if err != nil {
				return err
			}

			err = row.ScanNamed(
				query.Named("id", &e.ID),
				query.Named("payload_str", &e.PayloadStr),
				query.Named("payload_double", &e.PayloadDouble),
				query.Named("payload_timestamp", &e.PayloadTimestamp),
			)
			if err != nil {
				return err
			}

			return res.Err()
		},
		query.WithIdempotent(),
		query.WithTrace(trace.Query{
			OnDo: func(info trace.QueryDoStartInfo) func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
				return func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
					return func(info trace.QueryDoDoneInfo) {
						attempts = info.Attempts
					}
				}
			},
		}),
	)

	return e, attempts, err
}

func (s *Storage) Write(ctx context.Context, e generator.Row) (int, error) {
	var attempts int
	if err := ctx.Err(); err != nil {
		return attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	err := s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			_, res, err := session.Execute(ctx, s.upsertQuery,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(e.ID).
						Param("$payload_str").Text(*e.PayloadStr).
						Param("$payload_double").Double(*e.PayloadDouble).
						Param("$payload_timestamp").Timestamp(*e.PayloadTimestamp).
						Build(),
				),
				query.WithTxControl(writeTx),
			)
			if err != nil {
				return err
			}

			err = res.Err()
			if err != nil {
				return err
			}

			return res.Close(ctx)
		},
		query.WithIdempotent(),
		query.WithTrace(trace.Query{
			OnDo: func(info trace.QueryDoStartInfo) func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
				return func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
					return func(info trace.QueryDoDoneInfo) {
						attempts = info.Attempts
					}
				}
			},
		}),
	)

	return attempts, err
}

func (s *Storage) createTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		_, _, err := session.Execute(ctx, `
				CREATE TABLE `+"`"+path.Join(s.prefix, s.cfg.Table)+"`"+` (
					hash Uint64?,
					id Uint64?,
					payload_str Text?,
					payload_double Double?,
					payload_timestamp Timestamp?,
					payload_hash Uint64?,
					PRIMARY KEY (hash, id)
				) WITH (
					UNIFORM_PARTITIONS = `+strconv.FormatUint(s.cfg.MinPartitionsCount, 10)+`,
					AUTO_PARTITIONING_BY_SIZE = ENABLED,
					AUTO_PARTITIONING_PARTITION_SIZE_MB = `+strconv.FormatUint(s.cfg.PartitionSize, 10)+`,
					AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = `+strconv.FormatUint(s.cfg.MinPartitionsCount, 10)+`,
					AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = `+strconv.FormatUint(s.cfg.MaxPartitionsCount, 10)+`
				)
			`, query.WithTxControl(query.NoTx()))

		return err
	}, query.WithIdempotent())
}

func (s *Storage) dropTable(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			_, _, err := session.Execute(ctx, `
				DROP TABLE `+"`"+path.Join(s.prefix, s.cfg.Table)+"`"+`
			`)

			return err
		},
		query.WithIdempotent(),
	)
}

func (s *Storage) close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ShutdownTime)*time.Second)
	defer cancel()

	return s.db.Close(ctx)
}
