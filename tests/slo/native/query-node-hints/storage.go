package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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
DECLARE $payload_hash AS Uint64;
DECLARE $payload_double AS Double;
DECLARE $payload_timestamp AS Timestamp;

UPSERT INTO ` + "`%s`" + ` (
	id, payload_str, payload_double, payload_timestamp, payload_hash
) VALUES (
	$id, $payload_str, $payload_double, $payload_timestamp, $payload_hash
);
`

const readQuery = `
DECLARE $id AS Uint64;
SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
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
	control             generator.Row
	nodeHintMissesGauge otelmetric.Int64UpDownCounter
	createSQL           string
	dropSQL             string
}

// Storage implements Workload and Warmer
var (
	_ framework.Workload = (*Storage)(nil)
	_ framework.Warmer   = (*Storage)(nil)
)

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

	s := &Storage{
		fw:        fw,
		db:        db,
		params:    params,
		misses:    misses,
		gen:       generator.NewSeeded(120394832798), //nolint:mnd
		createSQL: fmt.Sprintf(createTableQuery, params.TablePath, params.MinPartitionCount),
		dropSQL:   fmt.Sprintf(dropTableQuery, params.TablePath),
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

func (s *Storage) reportNodeHintMisses() {
	if s.nodeHintMissesGauge != nil {
		s.nodeHintMissesGauge.Add(context.Background(), 1, otelmetric.WithAttributes(
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

	if err = s.initNodeSelector(ctx); err != nil {
		return fmt.Errorf("init node selector failed: %w", err)
	}

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

func (s *Storage) Warmup(ctx context.Context) error {
	s.fw.Logger.Printf("waiting 10s for partition stabilization...")
	time.Sleep(10 * time.Second) //nolint:mnd

	s.reportNodeHintMisses()

	ns := s.nodeSelector.Load()
	idx, nodeID := ns.GetRandomNodeID(s.gen)
	s.fw.Logger.Printf("all requests to node id: %d", nodeID)
	lower, upper := ns.LowerBounds[idx], ns.UpperBounds[idx]
	if upper-lower < 2 {
		return fmt.Errorf("selected partition has no separate control and load keys")
	}
	// Reserve the first key in this partition; arbitrary load reads may be absent.
	s.gen.SetRange(lower+1, upper)
	row, err := generator.New(lower).Generate()
	if err != nil {
		return err
	}
	row = kv.ControlRow(row)
	expected := kv.CloneRow(row)
	if _, _, err := s.write(ctx, row); err != nil {
		return err
	}
	actual, _, _, err := s.read(ctx, row.ID)
	if err != nil {
		return err
	}
	if err := kv.CheckRow(expected, actual); err != nil {
		return err
	}
	s.control = expected

	return nil
}

func (s *Storage) Run(ctx context.Context) error {
	readRL := framework.NewRateLimiter(int(s.params.ReadRPS))
	writeRL := framework.NewRateLimiter(int(s.params.WriteRPS))

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return framework.RunWorkers(ctx, int(s.params.ReadRPS), readRL, func(ctx context.Context) error {
			row, err := s.gen.Generate()
			if err != nil {
				s.fw.Logger.Errorf("generate error: %v", err)

				return err
			}
			span := s.fw.Metrics.Start(framework.OperationTypeRead)
			_, attempts, missed, err := s.read(ctx, row.ID)
			span.Finish(err, attempts)
			if missed {
				s.reportNodeHintMisses()
			}
			if err == nil {
				actual, _, controlMissed, controlErr := s.read(ctx, s.control.ID)
				if controlMissed {
					s.reportNodeHintMisses()
				}
				err = controlErr
				if err == nil {
					err = kv.CheckRow(s.control, actual)
				}
			}
			if err != nil && ctx.Err() == nil {
				s.fw.Logger.Errorf("read failed: %v", err)
			}

			return err
		})
	})

	group.Go(func() error {
		return framework.RunWorkers(ctx, int(s.params.WriteRPS), writeRL, func(ctx context.Context) error {
			row, err := s.gen.Generate()
			if err != nil {
				s.fw.Logger.Errorf("generate error: %v", err)

				return err
			}
			span := s.fw.Metrics.Start(framework.OperationTypeWrite)
			attempts, missed, err := s.write(ctx, row)
			span.Finish(err, attempts)
			if missed {
				s.reportNodeHintMisses()
			}
			if err != nil && ctx.Err() == nil {
				s.fw.Logger.Errorf("write failed: %v", err)
			}

			return err
		})
	})

	return group.Wait()
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

func (s *Storage) initNodeSelector(ctx context.Context) error {
	nsPtr, err := nodehints.RunUpdates(ctx, s.db, s.params.TablePath, time.Second*5) //nolint:mnd
	if err != nil {
		return fmt.Errorf("create node selector: %w", err)
	}
	s.nodeSelector = nsPtr

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
	var rowCount int

	err := s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) (err error) {
			e = generator.Row{}
			rowCount = 0
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

			for {
				rs, err := res.NextResultSet(ctx)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				for {
					row, err := rs.NextRow(ctx)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return err
					}
					e = generator.Row{}
					if err := row.ScanStruct(&e, query.WithScanStructAllowMissingColumnsFromSelect()); err != nil {
						return err
					}
					rowCount++
				}
			}
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

	if err == nil && rowCount > 1 {
		err = fmt.Errorf("data mismatch: id %d returned %d rows", entryID, rowCount)
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
						Param("$payload_hash").Uint64(*e.PayloadHash).
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
