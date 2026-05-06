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
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	maxID    = 100
	interval = time.Second
)

func dropTableIfExists(ctx context.Context, c query.Client, tablePath string) (err error) {
	return c.Exec(ctx,
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tablePath),
		query.WithTxControl(query.ImplicitTxControl()),
	)
}

func createTable(ctx context.Context, c query.Client, prefix, tableName string) (err error) {
	err = c.Exec(ctx,
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS `+"`%s`"+` (
				id Uint64,
				value Text,
				PRIMARY KEY (id)
			)`, path.Join(prefix, tableName)),
		query.WithTxControl(query.ImplicitTxControl()),
	)
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
`, prefix, tableName),
		query.WithTxControl(query.ImplicitTxControl()),
	)
	if err != nil {
		return fmt.Errorf("failed to add changefeed to test table: %w", err)
	}

	return nil
}

func fillTable(ctx context.Context, c query.Client, prefix, tableName string) {
	sql := fmt.Sprintf(`
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
		_ = c.DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			return tx.Exec(ctx, sql,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Any(types.Uint64Value(id)).
						Param("$value").Any(types.UTF8Value(val)).
						Build(),
				),
			)
		})

		time.Sleep(interval)
	}
}

func removeFromTable(ctx context.Context, c query.Client, prefix, tableName string) {
	sql := fmt.Sprintf(`
PRAGMA TablePathPrefix("%v");

DECLARE $id AS Uint64;

DELETE FROM
	%v
WHERE id=$id
`, prefix, tableName)
	for {
		id := uint64(rand.Intn(maxID)) //nolint:gosec
		_ = c.DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			return tx.Exec(ctx, sql,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Any(types.Uint64Value(id)).
						Build(),
				),
			)
		})

		time.Sleep(interval)
	}
}
