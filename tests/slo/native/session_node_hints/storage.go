package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sync/atomic"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/config"
	"slo/internal/generator"
	"slo/internal/node_hints"
)

type Storage struct {
	db           *ydb.Driver
	cfg          *config.Config
	misses       chan struct{}
	nodeSelector *atomic.Pointer[node_hints.NodeSelector]
	tablePath    string
	retryBudget  interface {
		budget.Budget

		Stop()
	}
}

const writeQuery = `
DECLARE $id AS Uint64;
DECLARE $payload_str AS Utf8;
DECLARE $payload_double AS Double;
DECLARE $payload_timestamp AS Timestamp;

UPSERT INTO ` + "`%s`" + ` (
	id, payload_str, payload_double, payload_timestamp
) VALUES (
	$id, $payload_str, $payload_double, $payload_timestamp
);
`

const readQuery = `
DECLARE $id AS Uint64;
SELECT id, payload_str, payload_double, payload_timestamp
FROM ` + "`%s`" + ` WHERE id = $id;
`

const createTableQuery = `
CREATE TABLE IF NOT EXISTS` + " `%s` " + `(
	id Uint64?,
	payload_str Text?,
	payload_double Double?,
	payload_timestamp Timestamp?,
	payload_hash Uint64?,
	PRIMARY KEY (id)
) WITH (
	UNIFORM_PARTITIONS = %d,
	AUTO_PARTITIONING_BY_SIZE = DISABLED,
	AUTO_PARTITIONING_BY_LOAD = DISABLED
)
`

const dropTableQuery = `
DROP TABLE %s
`

func NewStorage(ctx context.Context, cfg *config.Config, poolSize int, label string) (*Storage, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	retryBudget := budget.Limited(int(float64(poolSize) * 0.1)) //nolint:mnd

	misses := make(chan struct{})

	db, err := ydb.Open(ctx,
		cfg.Endpoint+cfg.DB,
		ydb.WithSessionPoolSizeLimit((cfg.ReadRPS+cfg.WriteRPS)/10),
		ydb.WithRetryBudget(retryBudget),
		ydb.WithTraceQuery(trace.Query{
			OnPoolGet: func(info trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
				return func(t trace.QueryPoolGetDoneInfo) {
					if t.NodeHintInfo != nil {
						if t.NodeHintInfo.SessionNodeID != t.NodeHintInfo.PreferredNodeID {
							misses <- struct{}{}
						}
					}
				}
			},
		}),
	)
	if err != nil {
		return nil, err
	}

	prefix := path.Join(db.Name(), label)

	tablePath := path.Join(prefix, cfg.Table)
	var nsPtr *atomic.Pointer[node_hints.NodeSelector]
	if cfg.Mode == config.RunMode {
		nsPtr, err = node_hints.RunUpdates(ctx, db, tablePath, time.Second*5)
		if err != nil {
			return nil, fmt.Errorf("create node selector: %w", err)
		}
	}

	s := &Storage{
		db:           db,
		cfg:          cfg,
		misses:       misses,
		tablePath:    path.Join(prefix, cfg.Table),
		retryBudget:  retryBudget,
		nodeSelector: nsPtr,
	}

	return s, nil
}

func (s *Storage) Read(ctx context.Context, entryID generator.RowID) (
	_ generator.Row,
	attempts int,
	missed bool,
	finalErr error,
) {
	if err := ctx.Err(); err != nil {
		return generator.Row{}, attempts, false, err
	}

	if s.nodeSelector != nil {
		ctx = s.nodeSelector.Load().WithNodeHint(ctx, entryID)
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
				)))
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
	select {
	case <-s.misses:
		missed = true
	default:
		missed = false
	}

	return e, attempts, missed, err
}

func (s *Storage) Write(ctx context.Context, e generator.Row) (attempts int, missed bool, finalErr error) {
	if err := ctx.Err(); err != nil {
		return attempts, false, err
	}

	if s.nodeSelector != nil {
		ctx = s.nodeSelector.Load().WithNodeHint(ctx, e.ID)
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
	select {
	case <-s.misses:
		missed = true
	default:
		missed = false
	}

	return attempts, missed, err
}

func (s *Storage) CreateTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	fmt.Printf(createTableQuery, s.tablePath, s.cfg.MinPartitionsCount)

	return s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx,
				fmt.Sprintf(createTableQuery, s.tablePath, s.cfg.MinPartitionsCount),
				query.WithTxControl(query.EmptyTxControl()),
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
				query.WithTxControl(query.EmptyTxControl()),
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
