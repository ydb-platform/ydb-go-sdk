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
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
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

func runPeriodically(ctx context.Context, every time.Duration, operation func() error) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := operation(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func fillTable(ctx context.Context, c table.Client, prefix, tableName string) error {
	query := fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

UPSERT INTO
	%v
	(id, value)
VALUES
	($id, $value)
`, prefix, tableName)

	return runPeriodically(ctx, interval, func() error {
		id := uint64(rand.Intn(maxID))              //nolint:gosec
		val := "val-" + strconv.Itoa(rand.Intn(10)) //nolint:gosec
		params := table.NewQueryParameters(
			table.ValueParam("$id", types.Uint64Value(id)),
			table.ValueParam("$value", types.UTF8Value(val)),
		)

		return c.DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, query, params, options.WithCommit())
			if err != nil {
				return err
			}

			return res.Close()
		}, table.WithIdempotent())
	})
}

func removeFromTable(ctx context.Context, c table.Client, prefix, tableName string) error {
	query := fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

DELETE FROM
	%v
WHERE id=$id
`, prefix, tableName)

	return runPeriodically(ctx, interval, func() error {
		id := uint64(rand.Intn(maxID)) //nolint:gosec
		params := table.NewQueryParameters(
			table.ValueParam("$id", types.Uint64Value(id)),
		)

		return c.DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, query, params, options.WithCommit())
			if err != nil {
				return err
			}

			return res.Close()
		}, table.WithIdempotent())
	})
}
