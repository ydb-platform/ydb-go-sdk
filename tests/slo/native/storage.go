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

	"slo/internal/config"
	"slo/internal/generator"
)

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

func (s *Storage) Read(ctx context.Context, entryID generator.RowID) (_ generator.Row, attempts int, err error) {
	if err = ctx.Err(); err != nil {
		return generator.Row{}, attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	e := generator.Row{}

	return e, attempts, s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			if err = ctx.Err(); err != nil {
				return err
			}

			var res result.Result
			_, res, err = session.Execute(ctx, readTx, s.selectQuery,
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
		table.WithIdempotent(),
		table.WithTrace(trace.Table{
			OnDo: func(info trace.TableDoStartInfo) func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
					return func(info trace.TableDoDoneInfo) {
						attempts = info.Attempts
					}
				}
			},
		}),
	)
}

func (s *Storage) Write(ctx context.Context, e generator.Row) (attempts int, err error) {
	if err := ctx.Err(); err != nil {
		return attempts, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return attempts, s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			_, res, err := session.Execute(ctx, writeTx, s.upsertQuery,
				table.NewQueryParameters(
					table.ValueParam("$id", types.Uint64Value(e.ID)),
					table.ValueParam("$payload_str", types.UTF8Value(*e.PayloadStr)),
					table.ValueParam("$payload_double", types.DoubleValue(*e.PayloadDouble)),
					table.ValueParam("$payload_timestamp", types.TimestampValueFromTime(*e.PayloadTimestamp)),
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
		table.WithTrace(trace.Table{
			OnDo: func(info trace.TableDoStartInfo) func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
					return func(info trace.TableDoDoneInfo) {
						attempts = info.Attempts
					}
				}
			},
		}),
	)
}

func (s *Storage) createTable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) error {
			return session.CreateTable(ctx, path.Join(s.prefix, s.cfg.Table),
				options.WithColumn("hash", types.Optional(types.TypeUint64)),
				options.WithColumn("id", types.Optional(types.TypeUint64)),
				options.WithColumn("payload_str", types.Optional(types.TypeUTF8)),
				options.WithColumn("payload_double", types.Optional(types.TypeDouble)),
				options.WithColumn("payload_timestamp", types.Optional(types.TypeTimestamp)),
				options.WithColumn("payload_hash", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("hash", "id"),

				options.WithPartitioningSettings(
					options.WithPartitioningBySize(options.FeatureEnabled),
					options.WithPartitionSizeMb(s.cfg.PartitionSize),
					options.WithMinPartitionsCount(s.cfg.MinPartitionsCount),
					options.WithMaxPartitionsCount(s.cfg.MaxPartitionsCount),
				),
				options.WithPartitions(options.WithUniformPartitions(s.cfg.MinPartitionsCount)),
			)
		},
		table.WithIdempotent(),
	)
}

func (s *Storage) dropTable(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			return session.DropTable(ctx, path.Join(s.prefix, s.cfg.Table))
		},
		table.WithIdempotent(),
	)
}

func (s *Storage) close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.ShutdownTime)*time.Second)
	defer cancel()

	return s.db.Close(ctx)
}
