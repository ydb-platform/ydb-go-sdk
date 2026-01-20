package main

import (
	"context"
	"fmt"
	"path"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/config"
	"slo/internal/generator"
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
	AUTO_PARTITIONING_BY_SIZE = ENABLED,
	AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,
	AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,
	AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d
)
`

const dropTableQuery = "DROP TABLE IF EXISTS `%s`;"

type Storage struct {
	db          *ydb.Driver
	cfg         *config.Config
	tablePath   string
	retryBudget interface {
		budget.Budget
		Stop()
	}
}

func NewStorage(ctx context.Context, cfg *config.Config, poolSize int, label string) (*Storage, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()

	retryBudget := budget.Limited(int(float64(poolSize) * 0.1)) //nolint:mnd

	db, err := ydb.Open(ctx,
		cfg.Endpoint+cfg.DB,
		ydb.WithSessionPoolSizeLimit(poolSize),
		ydb.WithRetryBudget(retryBudget),
		ydb.WithInsecure(),
		ydb.WithAnonymousCredentials(),
		ydb.WithTLSSInsecureSkipVerify(),
	)
	if err != nil {
		return nil, err
	}

	prefix := path.Join(db.Name(), label)

	s := &Storage{
		db:          db,
		cfg:         cfg,
		tablePath:   path.Join(prefix, cfg.Table),
		retryBudget: retryBudget,
	}

	return s, nil
}

func (s *Storage) WriteBatch(ctx context.Context, e []generator.Row) (attempts int, finalErr error) {
	if err := ctx.Err(); err != nil {
		return attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

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

	err := s.db.Table().BulkUpsert(
		ctx,
		s.tablePath,
		table.BulkUpsertDataRows(types.ListValue(rows...)),
		table.WithRetryOptions([]retry.Option{ //nolint:staticcheck
			retry.WithTrace(t),
		}),
		table.WithIdempotent(),
		table.WithLabel("WRITE"),
	)

	return attempts, err
}

func (s *Storage) ReadBatch(ctx context.Context, rowIDs []generator.RowID) (
	_ []generator.Row,
	attempts int,
	finalErr error,
) {
	if err := ctx.Err(); err != nil {
		return []generator.Row{}, attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	t := &trace.Retry{
		OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
			return func(info trace.RetryLoopDoneInfo) {
				attempts = info.Attempts
			}
		},
	}

	keys := make([]types.Value, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		key := types.StructValue(
			types.StructFieldValue("id", types.Uint64Value(rowID)),
		)
		keys = append(keys, key)
	}

	res, err := s.db.Table().ReadRows(ctx, s.tablePath, types.ListValue(keys...), []options.ReadRowsOption{},
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
			readRow := generator.Row{}
			scans := []named.Value{
				named.Required("id", &readRow.ID),
				named.Optional("payload_str", &readRow.PayloadStr),
				named.Optional("payload_double", &readRow.PayloadDouble),
				named.Optional("payload_timestamp", &readRow.PayloadTimestamp),
				named.Optional("payload_hash", &readRow.PayloadHash),
			}

			err = res.ScanNamed(scans...)
			if err != nil {
				return nil, attempts, err
			}

			readRows = append(readRows, readRow)
		}
	}

	return readRows, attempts, nil
}

func (s *Storage) CreateTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Query().Do(ctx,
		func(ctx context.Context, session query.Session) error {
			fmt.Println(fmt.Sprintf(createTableQuery, s.tablePath, s.cfg.MinPartitionsCount, s.cfg.PartitionSize,
				s.cfg.MinPartitionsCount, s.cfg.MaxPartitionsCount,
			))

			return session.Exec(ctx,
				fmt.Sprintf(createTableQuery, s.tablePath, s.cfg.MinPartitionsCount, s.cfg.PartitionSize,
					s.cfg.MinPartitionsCount, s.cfg.MaxPartitionsCount,
				),
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
