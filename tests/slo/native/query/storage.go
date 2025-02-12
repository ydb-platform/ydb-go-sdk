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
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/config"
	"slo/internal/generator"
)

type Storage struct {
	db          *ydb.Driver
	cfg         *config.Config
	tablePath   string
	retryBudget interface {
		budget.Budget

		Stop()
	}
}

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

const dropTableQuery = `
DROP TABLE %s
`

func NewStorage(ctx context.Context, cfg *config.Config, poolSize int, label string) (*Storage, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:gomnd
	defer cancel()

	retryBudget := budget.Limited(int(float64(poolSize) * 0.1)) //nolint:gomnd

	db, err := ydb.Open(ctx,
		cfg.Endpoint+cfg.DB,
		ydb.WithSessionPoolSizeLimit(poolSize),
		ydb.WithRetryBudget(retryBudget),
	)
	if err != nil {
		return nil, err
	}

	prefix := path.Join(db.Name(), label)

	s := &Storage{
		db:          db,
		cfg:         cfg,
		tablePath:   "`" + path.Join(prefix, cfg.Table) + "`",
		retryBudget: retryBudget,
	}

	return s, nil
}

func (s *Storage) Read(ctx context.Context, entryID generator.RowID) (_ generator.Row, attempts int, finalErr error) {
	if err := ctx.Err(); err != nil {
		return generator.Row{}, attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	e := generator.Row{}

	err := s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			res, err := session.Query(ctx,
				fmt.Sprintf(readQuery, s.tablePath),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(entryID).
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

			return nil
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

	return e, attempts, err
}

func (s *Storage) Write(ctx context.Context, e generator.Row) (attempts int, finalErr error) {
	if err := ctx.Err(); err != nil {
		return attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	err := s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) (err error) {
			return session.Exec(ctx,
				fmt.Sprintf(writeQuery, s.tablePath),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(e.ID).
						Param("$payload_str").Text(*e.PayloadStr).
						Param("$payload_double").Double(*e.PayloadDouble).
						Param("$payload_timestamp").Timestamp(*e.PayloadTimestamp).
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

func (s *Storage) CreateTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx,
				fmt.Sprintf(createTableQuery, s.tablePath, s.cfg.MinPartitionsCount, s.cfg.PartitionSize,
					s.cfg.MinPartitionsCount, s.cfg.MaxPartitionsCount,
				),
				query.WithTxControl(query.NoTx()),
			)
		}, query.WithIdempotent(),
		query.WithLabel("CREATE TABLE"),
	)
}

func (s *Storage) DropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx,
				fmt.Sprintf(dropTableQuery, s.tablePath),
				query.WithTxControl(query.NoTx()),
			)
		},
		query.WithIdempotent(),
		query.WithLabel("DROP TABLE"),
	)
}

func (s *Storage) Close(ctx context.Context) error {
	s.retryBudget.Stop()

	var (
		shutdownCtx    context.Context
		shutdownCancel context.CancelFunc
	)
	if s.cfg.ShutdownTime > 0 {
		shutdownCtx, shutdownCancel = context.WithTimeout(ctx, time.Duration(s.cfg.ShutdownTime)*time.Second)
	} else {
		shutdownCtx, shutdownCancel = context.WithCancel(ctx)
	}
	defer shutdownCancel()

	return s.db.Close(shutdownCtx)
}
