package main

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

const (
	maxID    = 100
	interval = time.Second
)

func dropTableIfExists(ctx context.Context, c query.Client, tablePath string) error {
	return c.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tablePath), query.WithIdempotent())
}

func createTable(ctx context.Context, c query.Client, prefix, tableName string) (err error) {
	err = c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id Uint64,
			value Text,
			PRIMARY KEY (id)
		)`, "`"+path.Join(prefix, tableName)+"`"), query.WithIdempotent())
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	err = c.Exec(ctx, fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

ALTER TABLE
	%v
ADD CHANGEFEED
	feed
WITH (
	FORMAT = 'JSON',
	MODE = 'NEW_AND_OLD_IMAGES'
)
`, prefix, tableName))
	if err != nil {
		return fmt.Errorf("failed to add changefeed to test table: %w", err)
	}

	return nil
}

func runPeriodically(ctx context.Context, every time.Duration, operation func(context.Context) error) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := operation(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func fillTable(ctx context.Context, c query.Client, prefix, tableName string) error {
	sql := fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

UPSERT INTO
	%v
	(id, value)
VALUES
	($id, $value)
`, prefix, tableName)

	return runPeriodically(ctx, interval, func(ctx context.Context) error {
		id := uint64(rand.Intn(maxID))              //nolint:gosec
		val := "val-" + strconv.Itoa(rand.Intn(10)) //nolint:gosec

		return c.DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			return tx.Exec(ctx, sql, query.WithParameters(ydb.ParamsBuilder().
				Param("$id").Uint64(id).
				Param("$value").Text(val).
				Build()))
		}, query.WithIdempotent())
	})
}

func removeFromTable(ctx context.Context, c query.Client, prefix, tableName string) error {
	sql := fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

DELETE FROM
	%v
WHERE id=$id
`, prefix, tableName)

	return runPeriodically(ctx, interval, func(ctx context.Context) error {
		id := uint64(rand.Intn(maxID)) //nolint:gosec

		return c.DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			return tx.Exec(ctx, sql, query.WithParameters(ydb.ParamsBuilder().
				Param("$id").Uint64(id).
				Build()))
		}, query.WithIdempotent())
	})
}
