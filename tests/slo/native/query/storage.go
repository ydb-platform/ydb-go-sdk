package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/config"
	"slo/internal/generator"
)

type Storage struct {
	db        *ydb.Driver
	cfg       *config.Config
	tablePath string
}

func NewStorage(ctx context.Context, cfg *config.Config, poolSize int) (*Storage, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	db, err := ydb.Open(ctx,
		cfg.Endpoint+cfg.DB,
		ydb.WithSessionPoolMaxSize(poolSize),
		ydb.WithSessionPoolMinSize(poolSize/10),
		ydb.WithSessionPoolProducersCount(poolSize/10),
	)
	if err != nil {
		return nil, err
	}

	prefix := path.Join(db.Name(), label)

	s := &Storage{
		db:        db,
		cfg:       cfg,
		tablePath: "`" + path.Join(prefix, cfg.Table) + "`",
	}

	return s, nil
}

func (s *Storage) Read(ctx context.Context, entryID generator.RowID) (_ generator.Row, attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return generator.Row{}, attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	e := generator.Row{}

	err = s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			_, res, err := session.Execute(ctx,
				fmt.Sprintf(`
					DECLARE $id AS Uint64;
					SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
					FROM %s WHERE id = $id AND hash = Digest::NumericHash($id);
				`, s.tablePath),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(entryID).
						Build(),
				),
				query.WithTxControl(query.TxControl(
					query.BeginTx(query.WithOnlineReadOnly()),
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

			row, err := rs.NextRow(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}

				return err
			}

			err = row.ScanStruct(&e, query.WithScanStructAllowMissingColumnsFromSelect())
			if err != nil {
				return err
			}

			return res.Err()
		},
		query.WithIdempotent(),
		query.WithTrace(&trace.Query{
			OnDo: func(info trace.QueryDoStartInfo) func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
				return func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
					return func(info trace.QueryDoDoneInfo) {
						attempts = info.Attempts
					}
				}
			},
		}),
		query.WithLabel("READ"),
	)

	return e, attempts, err
}

func (s *Storage) Write(ctx context.Context, e generator.Row) (attempts int, _ error) {
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

			_, res, err := session.Execute(ctx,
				fmt.Sprintf(`
					DECLARE $id AS Uint64;
					DECLARE $payload_str AS Utf8;
					DECLARE $payload_double AS Double;
					DECLARE $payload_timestamp AS Timestamp;
					
					UPSERT INTO %s (
						id, hash, payload_str, payload_double, payload_timestamp
					) VALUES (
						$id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp
					);
				`, s.tablePath),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(e.ID).
						Param("$payload_str").Text(*e.PayloadStr).
						Param("$payload_double").Double(*e.PayloadDouble).
						Param("$payload_timestamp").Timestamp(*e.PayloadTimestamp).
						Build(),
				),
			)
			if err != nil {
				return err
			}

			defer func() {
				_ = res.Close(ctx)
			}()

			return res.Err()
		},
		query.WithIdempotent(),
		query.WithTrace(&trace.Query{
			OnDo: func(info trace.QueryDoStartInfo) func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
				return func(info trace.QueryDoIntermediateInfo) func(trace.QueryDoDoneInfo) {
					return func(info trace.QueryDoDoneInfo) {
						attempts = info.Attempts
					}
				}
			},
		}),
		query.WithLabel("WRITE"),
	)

	return attempts, err
}

func (s *Storage) createTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			_, _, err := session.Execute(ctx,
				fmt.Sprintf(`
				CREATE TABLE %s (
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
				)`, s.tablePath, s.cfg.MinPartitionsCount, s.cfg.PartitionSize,
					s.cfg.MinPartitionsCount, s.cfg.MaxPartitionsCount,
				),
				query.WithTxControl(query.NoTx()))

			return err
		}, query.WithIdempotent(),
		query.WithLabel("CREATE TABLE"),
	)
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
			_, _, err := session.Execute(ctx,
				fmt.Sprintf(`DROP TABLE %s`, s.tablePath),
				query.WithTxControl(query.NoTx()),
			)

			return err
		},
		query.WithIdempotent(),
		query.WithLabel("DROP TABLE"),
	)
}

func (s *Storage) close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ShutdownTime)*time.Second)
	defer cancel()

	return s.db.Close(ctx)
}
