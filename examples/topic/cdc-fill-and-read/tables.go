package main

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	maxID    = 100
	interval = time.Second
)

func dropTableIfExists(ctx context.Context, c table.Client, path string) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.DropTable(ctx, path)
		},
		table.WithIdempotent(),
	)
	if !ydb.IsOperationErrorSchemeError(err) {
		return err
	}
	return nil
}

func createTable(ctx context.Context, c table.Client, prefix, tableName string) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, tableName),
				options.WithColumn("id", types.Optional(types.TypeUint64)),
				options.WithColumn("value", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("id"),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	err = c.Do(ctx, func(ctx context.Context, s table.Session) error {
		query := fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

ALTER TABLE
	%v
ADD CHANGEFEED
	feed
WITH (
	FORMAT = 'JSON',
	MODE = 'NEW_AND_OLD_IMAGES'
)
`, prefix, tableName)
		return s.ExecuteSchemeQuery(ctx, query)
	})
	if err != nil {
		return fmt.Errorf("failed to add changefeed to test table: %w", err)
	}
	return nil
}

func fillTable(ctx context.Context, c table.Client, prefix, tableName string) {
	query := fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

DECLARE $id AS Uint64;
DECLARE $value AS Text;

UPSERT INTO
	%v
	(id, value)
VALUES
	($id, $value)
`, prefix, tableName)
	for {
		id := uint64(rand.Intn(maxID))              //nolint:gosec
		val := "val-" + strconv.Itoa(rand.Intn(10)) //nolint:gosec
		params := table.NewQueryParameters(
			table.ValueParam("$id", types.Uint64Value(id)),
			table.ValueParam("$value", types.UTF8Value(val)),
		)
		_ = c.DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, query, params)
			return err
		})

		time.Sleep(interval)
	}
}

func removeFromTable(ctx context.Context, c table.Client, prefix, tableName string) {
	query := fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

DECLARE $id AS Uint64;

DELETE FROM
	%v
WHERE id=$id
`, prefix, tableName)
	for {
		id := uint64(rand.Intn(maxID)) //nolint:gosec
		params := table.NewQueryParameters(
			table.ValueParam("$id", types.Uint64Value(id)),
		)
		_ = c.DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, query, params)
			return err
		})

		time.Sleep(interval)
	}
}
