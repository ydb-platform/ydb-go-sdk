package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"time"

	"slo/internal/configs"
	"slo/internal/generator"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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

var ErrUnableToParseID = errors.New("unable to parse entry id as uuid")

type Storage struct {
	db          *ydb.Driver
	cfg         configs.Config
	upsertQuery string
	selectQuery string
}

func NewStorage(ctx context.Context, cfg configs.Config) (st Storage, err error) {
	localCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	st.cfg = cfg
	st.db, err = ydb.Open(localCtx, st.cfg.Endpoint+st.cfg.DB, ydb.WithAccessTokenCredentials(st.cfg.YDBToken))
	if err != nil {
		return Storage{}, err
	}

	st.upsertQuery = fmt.Sprintf(upsertTemplate, st.cfg.Table)
	st.selectQuery = fmt.Sprintf(selectTemplate, st.cfg.Table)

	return
}

func (st *Storage) Close(ctx context.Context) error {
	return st.db.Close(ctx)
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
				err := res.Close()
				if err != nil {
					log.Printf("close failed: %v", err)
				}
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

	return
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
