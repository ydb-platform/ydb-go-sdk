package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"slo/internal/framework"
	"slo/internal/generator"
	"slo/internal/kv"
	"slo/internal/nodehints"
)

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

const dropTableQuery = "DROP TABLE IF EXISTS `%s`;"

type Storage struct {
	fw                  *framework.Framework
	db                  *ydb.Driver
	params              kv.Params
	misses              chan struct{}
	nodeSelector        *atomic.Pointer[nodehints.NodeSelector]
	gen                 *generator.SeededGenerator
	nodeHintMissesGauge otelmetric.Int64UpDownCounter
	createSQL           string
	dropSQL             string
}

// Storage implements Workload
var _ framework.Workload = (*Storage)(nil)

func NewStorage(ctx context.Context, fw *framework.Framework) (*Storage, error) {
	params := kv.ParseParams(fw, "query-node-hints", nil)

	misses := make(chan struct{})

	connectCtx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	db, err := ydb.Open(connectCtx,
		fw.Config.Endpoint+fw.Config.Database,
		ydb.WithSessionPoolSizeLimit(params.PoolSize()),
		ydb.WithRetryBudget(params.RetryBudget),
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

	nsPtr, err := nodehints.RunUpdates(ctx, db, params.TablePath, time.Second*5) //nolint:mnd
	if err != nil {
		return nil, fmt.Errorf("create node selector: %w", err)
	}

	s := &Storage{
		fw:           fw,
		db:           db,
		params:       params,
		misses:       misses,
		nodeSelector: nsPtr,
		gen:          generator.NewSeeded(120394832798), //nolint:mnd
		createSQL:    fmt.Sprintf(createTableQuery, params.TablePath, params.MinPartitionCount),
		dropSQL:      fmt.Sprintf(dropTableQuery, params.TablePath),
	}

	if fw.Metrics.Meter() != nil {
		s.nodeHintMissesGauge, err = fw.Metrics.Meter().Int64UpDownCounter(
			"workload.nodehints.misses",
			otelmetric.WithDescription("Exclusively for node_hints SLO workload: Signals GRPC requests to wrong node"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create nodeHintMissesPresent counter: %w", err)
		}
	}

	return s, nil
}

func (s *Storage) reportNodeHintMisses(val int64) {
	if s.nodeHintMissesGauge != nil {
		s.nodeHintMissesGauge.Add(context.Background(), val, otelmetric.WithAttributes(
			attribute.String("ref", s.fw.Config.Ref),
		))
	}
}

func (s *Storage) Setup(ctx context.Context) error {
	err := s.createTable(ctx)
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	s.fw.Logger.Printf("create table ok")

	g := errgroup.Group{}
	for i := uint64(0); i < s.params.PrefillCount; i++ {
		g.Go(func() error {
			e, err := s.gen.Generate()
			if err != nil {
				return err
			}
			_, _, err = s.write(ctx, e)

			return err
		})
	}
	if err = g.Wait(); err != nil {
		return fmt.Errorf("fill initial data failed: %w", err)
	}
	s.fw.Logger.Printf("entries write ok")

	return nil
}

func (s *Storage) Run(ctx context.Context) error {
	readRL := framework.NewRateLimiter(int(s.params.ReadRPS))
	writeRL := framework.NewRateLimiter(int(s.params.WriteRPS))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		framework.RunWorkers(ctx, int(s.params.ReadRPS), readRL, func(ctx context.Context) {
			row, err := s.gen.Generate()
			if err != nil {
				s.fw.Logger.Errorf("generate error: %v", err)

				return
			}
			span := s.fw.Metrics.Start(framework.OperationTypeRead)
			_, attempts, missed, err := s.read(ctx, row.ID)
			span.Finish(err, attempts)
			if missed {
				s.reportNodeHintMisses(1)
			}
			if err != nil && ctx.Err() == nil {
				s.fw.Logger.Errorf("read failed: %v", err)
			}
		})
	}()

	go func() {
		defer wg.Done()
		framework.RunWorkers(ctx, int(s.params.WriteRPS), writeRL, func(ctx context.Context) {
			row, err := s.gen.Generate()
			if err != nil {
				s.fw.Logger.Errorf("generate error: %v", err)

				return
			}
			span := s.fw.Metrics.Start(framework.OperationTypeWrite)
			attempts, missed, err := s.write(ctx, row)
			span.Finish(err, attempts)
			if missed {
				s.reportNodeHintMisses(1)
			}
			if err != nil && ctx.Err() == nil {
				s.fw.Logger.Errorf("write failed: %v", err)
			}
		})
	}()

	wg.Wait()

	return nil
}

func (s *Storage) Teardown(ctx context.Context) error {
	defer func() {
		s.params.RetryBudget.Stop()
		_ = s.db.Close(ctx)
	}()

	if err := s.dropTable(ctx); err != nil {
		return fmt.Errorf("drop table failed: %w", err)
	}
	s.fw.Logger.Printf("cleanup table ok")

	return nil
}

func (s *Storage) read(ctx context.Context, entryID generator.RowID) (
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

	ctx, cancel := context.WithTimeout(ctx, s.params.ReadTimeout)
	defer cancel()

	e := generator.Row{}

	err := s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}
			res, err := session.Query(ctx,
				fmt.Sprintf(readQuery, s.params.TablePath),
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

			return row.ScanStruct(&e, query.WithScanStructAllowMissingColumnsFromSelect())
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

func (s *Storage) write(ctx context.Context, e generator.Row) (attempts int, missed bool, _ error) {
	if err := ctx.Err(); err != nil {
		return attempts, false, err
	}

	if s.nodeSelector != nil {
		ctx = s.nodeSelector.Load().WithNodeHint(ctx, e.ID)
	}
	ctx, cancel := context.WithTimeout(ctx, s.params.WriteTimeout)
	defer cancel()

	err := s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) (err error) {
			return session.Exec(ctx,
				fmt.Sprintf(writeQuery, s.params.TablePath),
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

func (s *Storage) createTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.params.WriteTimeout)
	defer cancel()

	return s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx, s.createSQL, query.WithTxControl(query.EmptyTxControl()))
		}, query.WithIdempotent(),
		query.WithLabel("CREATE TABLE"),
	)
}

func (s *Storage) dropTable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, s.params.WriteTimeout)
	defer cancel()

	return s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			return session.Exec(ctx, s.dropSQL, query.WithTxControl(query.EmptyTxControl()))
		},
		query.WithIdempotent(),
		query.WithLabel("DROP TABLE"),
	)
}
