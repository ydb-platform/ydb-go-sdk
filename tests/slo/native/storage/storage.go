package storage

import (
	"context"
	"fmt"
	"path"
	"time"

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
DECLARE $id AS Utf8;
DECLARE $payload AS Utf8;
UPSERT INTO %s (id, payload) VALUES ($id, $payload);
`
	selectTemplate = `
DECLARE $id AS UTf8;
SELECT id, payload
FROM %s WHERE id = $id;
`
)

type Storage struct {
	db          *ydb.Driver
	cfg         config.Config
	upsertQuery string
	selectQuery string
}

func New(ctx context.Context, cfg config.Config, logger *zap.Logger, poolSize int) (_ Storage, err error) {
	localCtx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	st := Storage{
		cfg:         cfg,
		upsertQuery: fmt.Sprintf(upsertTemplate, cfg.Table),
		selectQuery: fmt.Sprintf(selectTemplate, cfg.Table),
	}
	st.db, err = ydb.Open(
		localCtx,
		st.cfg.Endpoint+st.cfg.DB,
		ydb.WithAccessTokenCredentials(st.cfg.YDBToken),
		ydbZap.WithTraces(
			logger,
			trace.DetailsAll,
		),
		ydb.WithSessionPoolSizeLimit(poolSize),
	)
	if err != nil {
		return Storage{}, err
	}

	g := errgroup.Group{}

	for i := 0; i < poolSize; i++ {
		g.Go(func() error {
			err := st.db.Table().Do(localCtx, func(ctx context.Context, s table.Session) error {
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
		return Storage{}, err
	}

	return st, nil
}

func (st *Storage) Close(ctx context.Context) error {
	ctxLocal, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return st.db.Close(ctxLocal)
}

func (st *Storage) CreateTable(ctx context.Context) (err error) {
	ctxLocal, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = st.db.Table().Do(ctxLocal,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(st.db.Name(), st.cfg.Table),
				options.WithColumn("id", types.Optional(types.TypeUTF8)),
				options.WithColumn("payload", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("id"),
			)
		},
	)

	return
}

func (st *Storage) DropTable(ctx context.Context) (err error) {
	ctxLocal, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = st.db.Table().Do(ctxLocal,
		func(ctx context.Context, s table.Session) (err error) {
			return s.DropTable(ctx, path.Join(st.db.Name(), st.cfg.Table))
		},
	)

	return
}

func (st *Storage) Read(ctx context.Context, entryID generator.EntryID) (e generator.Entry, err error) {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(st.cfg.ReadTimeout)*time.Millisecond)
	defer cancel()

	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	err = st.db.Table().Do(ctxLocal,
		func(ctx context.Context, s table.Session) (err error) {
			var res result.Result
			_, res, err = s.Execute(ctx, readTx, st.selectQuery,
				table.NewQueryParameters(
					table.ValueParam("$id", types.UTF8Value(entryID.String())),
				),
			)
			if err != nil {
				return err
			}
			defer func(res result.Result) {
				_ = res.Close()
			}(res)
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					var payload *string

					err = res.ScanNamed(
						named.Required("id", &e.ID),
						named.Optional("payload", &payload),
					)
					if err != nil {
						return err
					}

					e.Payload = *payload
				}
			}
			return res.Err()
		},
	)

	return e, err
}

func (st *Storage) Write(ctx context.Context, e generator.Entry) error {
	ctxLocal, cancel := context.WithTimeout(ctx, time.Duration(st.cfg.WriteTimeout)*time.Millisecond)
	defer cancel()

	return st.db.Table().DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) (err error) {
			res, err := tx.Execute(ctxLocal, st.upsertQuery,
				table.NewQueryParameters(
					table.ValueParam("$id", types.UTF8Value(e.ID.String())),
					table.ValueParam("$payload", types.UTF8Value(e.Payload)),
				),
			)
			if err != nil {
				return err
			}
			if err = res.Err(); err != nil {
				return err
			}
			return res.Close()
		}, table.WithIdempotent(),
	)
}
