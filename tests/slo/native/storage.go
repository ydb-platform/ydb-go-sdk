package main

import (
	"context"
	"fmt"
	"path"
	"time"

	env "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"slo/internal/config"
	"slo/internal/generator"
)

const (
	upsertTemplate = `
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
	selectTemplate = `
DECLARE $id AS Uint64;
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
	db          *ydb.Driver
	cfg         *config.Config
	upsertQuery string
	selectQuery string
}

func NewStorage(ctx context.Context, cfg *config.Config, logger *zap.Logger, poolSize int) (*Storage, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	st := &Storage{
		cfg:         cfg,
		upsertQuery: fmt.Sprintf(upsertTemplate, cfg.Table),
		selectQuery: fmt.Sprintf(selectTemplate, cfg.Table),
	}
	var err error
	st.db, err = ydb.Open(
		ctx,
		st.cfg.Endpoint+st.cfg.DB,
		env.WithEnvironCredentials(ctx),
		ydbZap.WithTraces(
			logger,
			trace.DetailsAll,
		),
		ydb.WithSessionPoolSizeLimit(poolSize),
	)
	if err != nil {
		return nil, err
	}

	g := errgroup.Group{}

	for i := 0; i < poolSize; i++ {
		g.Go(func() error {
			err := st.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				return nil
			})
			if err != nil {
				return fmt.Errorf("error when create session: %w", err)
			}
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	return st, nil
}

func (st *Storage) Close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	return st.db.Close(ctx)
}

func (st *Storage) CreateTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(st.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return st.db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(st.db.Name(), st.cfg.Table),
				options.WithColumn("hash", types.Optional(types.TypeUint64)),
				options.WithColumn("id", types.Optional(types.TypeUint64)),
				options.WithColumn("payload_str", types.Optional(types.TypeUTF8)),
				options.WithColumn("payload_double", types.Optional(types.TypeDouble)),
				options.WithColumn("payload_timestamp", types.Optional(types.TypeTimestamp)),
				options.WithColumn("payload_hash", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("hash", "id"),

				options.WithPartitioningSettings(
					options.WithPartitioningBySize(options.FeatureEnabled),
					options.WithPartitionSizeMb(1),
					options.WithMinPartitionsCount(st.cfg.PartitionsCount),
					options.WithMaxPartitionsCount(1000),
				),
				options.WithProfile(
					options.WithPartitioningPolicy(
						options.WithPartitioningPolicyUniformPartitions(st.cfg.PartitionsCount),
					),
				),
			)
		},
	)
}

func (st *Storage) DropTable(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(st.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return st.db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(st.db.Name(), st.cfg.Table))
		},
	)
}

func (st *Storage) Read(ctx context.Context, entryID generator.RowID) (generator.Row, error) {
	if err := ctx.Err(); err != nil {
		return generator.Row{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(st.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	e := generator.Row{}

	err := st.db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			var res result.Result
			_, res, err = s.Execute(ctx, readTx, st.selectQuery,
				table.NewQueryParameters(
					table.ValueParam("$id", types.Uint64Value(entryID)),
				),
			)
			if err != nil {
				return err
			}
			defer func(res result.Result) {
				_ = res.Close()
			}(res)

			err = res.NextResultSetErr(ctx)
			if err != nil {
				return err
			}

			if !res.NextRow() {
				return fmt.Errorf("entry not found, id = %v", entryID)
			}

			err = res.ScanNamed(
				named.Required("id", &e.ID),
				named.Optional("payload_str", &e.PayloadStr),
				named.Optional("payload_double", &e.PayloadDouble),
				named.Optional("payload_timestamp", &e.PayloadTimestamp),
			)
			if err != nil {
				return err
			}

			return res.Err()
		},
	)

	return e, err
}

func (st *Storage) Write(ctx context.Context, e generator.Row) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(st.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return st.db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			_, res, err := s.Execute(ctx, writeTx, st.upsertQuery,
				table.NewQueryParameters(
					table.ValueParam("$id", types.Uint64Value(e.ID)),
					table.ValueParam("$payload_str", types.UTF8Value(*e.PayloadStr)),
					table.ValueParam("$payload_double", types.DoubleValue(*e.PayloadDouble)),
					table.ValueParam("$payload_timestamp", types.TimestampValue(*e.PayloadTimestamp)),
				),
			)
			if err != nil {
				return err
			}

			err = res.Err()
			if err != nil {
				return err
			}

			return res.Close()
		},
		table.WithIdempotent(),
	)
}
