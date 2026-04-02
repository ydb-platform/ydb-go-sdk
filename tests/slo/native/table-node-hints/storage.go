package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"slo/internal/framework"
	"slo/internal/generator"
	"slo/internal/kv"
	"slo/internal/nodehints"
)

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
	batchSize           uint
	nodeSelector        *atomic.Pointer[nodehints.NodeSelector]
	gen                 *generator.SeededGenerator
	nodeHintMissesGauge otelmetric.Int64UpDownCounter
	createSQL           string
	dropSQL             string
}

// Storage implements Workload, Warmer, and Cooler
var (
	_ framework.Workload = (*Storage)(nil)
	_ framework.Warmer   = (*Storage)(nil)
	_ framework.Cooler   = (*Storage)(nil)
)

func NewStorage(ctx context.Context, fw *framework.Framework) (*Storage, error) {
	var batchSize uint

	params := kv.ParseParams(fw, "table-node-hints", func(fs *flag.FlagSet) {
		fs.UintVar(&batchSize, "batch-size", 10, "batch size for bulk operations")
	})

	connectCtx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	db, err := ydb.Open(connectCtx,
		fw.Config.Endpoint+fw.Config.Database,
		ydb.WithSessionPoolSizeLimit(params.PoolSize()),
		ydb.WithRetryBudget(params.RetryBudget),
	)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		fw:        fw,
		db:        db,
		params:    params,
		batchSize: batchSize,
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
			_, err = s.writeBatch(ctx, []generator.Row{e})

			return err
		})
	}
	if err = g.Wait(); err != nil {
		return fmt.Errorf("fill initial data failed: %w", err)
	}
	s.fw.Logger.Printf("entries write ok")

	return nil
}

func (s *Storage) Warmup(_ context.Context) error {
	s.fw.Logger.Printf("waiting 10s for partition stabilization...")
	time.Sleep(10 * time.Second) //nolint:mnd

	s.reportNodeHintMisses(1)

	ns := s.nodeSelector.Load()
	idx, nodeID := ns.GetRandomNodeID(s.gen)
	s.fw.Logger.Printf("all requests to node id: %d", nodeID)
	s.gen.SetRange(ns.LowerBounds[idx], ns.UpperBounds[idx])

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
			ids := make([]generator.RowID, 0, s.batchSize)
			for range s.batchSize {
				row, err := s.gen.Generate()
				if err != nil {
					s.fw.Logger.Errorf("generate error: %v", err)

					return
				}
				ids = append(ids, row.ID)
			}
			span := s.fw.Metrics.Start(framework.OperationTypeRead)
			_, attempts, err := s.readBatch(ctx, ids)
			span.Finish(err, attempts)
			if err != nil && ctx.Err() == nil {
				s.fw.Logger.Errorf("read failed: %v", err)
			}
		})
	}()

	go func() {
		defer wg.Done()
		framework.RunWorkers(ctx, int(s.params.WriteRPS), writeRL, func(ctx context.Context) {
			rows := make([]generator.Row, 0, s.batchSize)
			for range s.batchSize {
				row, err := s.gen.Generate()
				if err != nil {
					s.fw.Logger.Errorf("generate error: %v", err)

					return
				}
				rows = append(rows, row)
			}
			span := s.fw.Metrics.Start(framework.OperationTypeWrite)
			attempts, err := s.writeBatch(ctx, rows)
			span.Finish(err, attempts)
			if err != nil && ctx.Err() == nil {
				s.fw.Logger.Errorf("write failed: %v", err)
			}
		})
	}()

	wg.Wait()

	return nil
}

func (s *Storage) Cooldown(ctx context.Context) error {
	ns := s.nodeSelector.Load()
	_, nodeID := ns.GetRandomNodeID(s.gen)

	estimator := NewEstimator(ctx, s)
	ectx, ecancel := context.WithTimeout(context.Background(), 30*time.Second) //nolint:mnd
	defer ecancel()

	if err := estimator.OnlyThisNode(ectx, nodeID); err == nil {
		s.reportNodeHintMisses(-1)
	} else {
		return fmt.Errorf("failed to verify load went to a single node: %w", err)
	}

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

func (s *Storage) initNodeSelector(ctx context.Context) error {
	nsPtr, err := nodehints.RunUpdates(ctx, s.db, s.params.TablePath, time.Second*5) //nolint:mnd
	if err != nil {
		return fmt.Errorf("create node selector: %w", err)
	}
	s.nodeSelector = nsPtr

	return nil
}

func (s *Storage) writeBatch(ctx context.Context, e []generator.Row) (attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	rows := make([]types.Value, 0, len(e))
	for _, row := range e {
		rows = append(rows, types.StructValue(
			types.StructFieldValue("id", types.Uint64Value(row.ID)),
			types.StructFieldValue("payload_str", types.OptionalValue(types.TextValue(*row.PayloadStr))),
			types.StructFieldValue("payload_double", types.OptionalValue(types.DoubleValue(*row.PayloadDouble))),
			types.StructFieldValue(
				"payload_timestamp",
				types.OptionalValue(types.TimestampValue(uint64(row.PayloadTimestamp.UnixMicro()))),
			),
			types.StructFieldValue("payload_hash", types.OptionalValue(types.Uint64Value(*row.PayloadHash))),
		))
	}

	t := &trace.Retry{
		OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
			return func(info trace.RetryLoopDoneInfo) {
				attempts = info.Attempts
			}
		},
	}

	reqCtx := ctx
	if s.nodeSelector != nil {
		reqCtx = s.nodeSelector.Load().WithNodeHint(ctx, e[0].ID)
	}
	reqCtx, cancel := context.WithTimeout(reqCtx, s.params.WriteTimeout)
	defer cancel()

	err := s.db.Table().BulkUpsert(
		reqCtx,
		s.params.TablePath,
		table.BulkUpsertDataRows(types.ListValue(rows...)),
		table.WithRetryOptions([]retry.Option{ //nolint:staticcheck
			retry.WithTrace(t),
		}),
		table.WithIdempotent(),
		table.WithLabel("WRITE"),
	)

	return attempts, err
}

func (s *Storage) readBatch(ctx context.Context, rowIDs []generator.RowID) (_ []generator.Row, attempts int, _ error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	t := &trace.Retry{
		OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
			return func(info trace.RetryLoopDoneInfo) {
				attempts = info.Attempts
			}
		},
	}

	keys := make([]types.Value, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		keys = append(keys, types.StructValue(
			types.StructFieldValue("id", types.Uint64Value(rowID)),
		))
	}

	reqCtx := ctx
	if s.nodeSelector != nil {
		reqCtx = s.nodeSelector.Load().WithNodeHint(ctx, rowIDs[0])
	}
	reqCtx, cancel := context.WithTimeout(reqCtx, s.params.ReadTimeout)
	defer cancel()

	res, err := s.db.Table().ReadRows(reqCtx, s.params.TablePath, types.ListValue(keys...), []options.ReadRowsOption{},
		table.WithRetryOptions([]retry.Option{ //nolint:staticcheck
			retry.WithTrace(t),
		}),
		table.WithIdempotent(),
		table.WithLabel("READ"),
	)
	if err != nil {
		return nil, attempts, err
	}
	defer func() {
		_ = res.Close()
	}()

	readRows := make([]generator.Row, 0, len(rowIDs))
	for res.NextResultSet(ctx) {
		if err = res.Err(); err != nil {
			return nil, attempts, err
		}

		if res.CurrentResultSet().Truncated() {
			return nil, attempts, fmt.Errorf("read rows result set truncated")
		}

		for res.NextRow() {
			var row generator.Row
			err = res.ScanNamed(
				named.Required("id", &row.ID),
				named.Optional("payload_str", &row.PayloadStr),
				named.Optional("payload_double", &row.PayloadDouble),
				named.Optional("payload_timestamp", &row.PayloadTimestamp),
				named.Optional("payload_hash", &row.PayloadHash),
			)
			if err != nil {
				return nil, attempts, err
			}

			readRows = append(readRows, row)
		}
	}

	return readRows, attempts, nil
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
