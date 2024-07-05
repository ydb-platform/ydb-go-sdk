//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTableCreateTablePartitions(sourceTest *testing.T) {
	t := xtest.MakeSyncedTest(sourceTest)
	ctx := xtest.Context(t)

	db, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithLogger(
			newLoggerWithMinLevel(t, log.WARN),
			trace.MatchDetails(`ydb\.(driver|discovery|retry|scheme).*`),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	t.Run("uniform partitions", func(t *testing.T) {
		err := db.Table().Do(ctx,
			func(ctx context.Context, session table.Session) error {
				tablePath := path.Join(db.Name(), "uniformTable")

				err := session.CreateTable(ctx, tablePath,
					options.WithColumn("key", types.Optional(types.TypeUint64)),
					options.WithColumn("value", types.Optional(types.TypeJSON)),
					options.WithPrimaryKeyColumn("key"),

					options.WithPartitions(options.WithUniformPartitions(4)),
				)
				if err != nil {
					return fmt.Errorf("failed to create table: %w", err)
				}

				desc, err := session.DescribeTable(ctx, tablePath, options.WithShardKeyBounds())
				if err != nil {
					return fmt.Errorf("failed to get table description: %w", err)
				}
				if len(desc.KeyRanges) != 4 {
					return errors.New("key ranges len is not as expected")
				}

				return nil
			},
		)
		assert.NoError(t, err)
	})

	t.Run("explicit partitions", func(t *testing.T) {
		err := db.Table().Do(ctx,
			func(ctx context.Context, session table.Session) error {
				tablePath := path.Join(db.Name(), "explicitTable")

				err := session.CreateTable(ctx, tablePath,
					options.WithColumn("key", types.Optional(types.TypeUint64)),
					options.WithColumn("value", types.Optional(types.TypeJSON)),
					options.WithPrimaryKeyColumn("key"),

					options.WithPartitions(
						options.WithExplicitPartitions(
							types.TupleValue(types.OptionalValue(types.Uint64Value(100))),
							types.TupleValue(types.OptionalValue(types.Uint64Value(300))),
							types.TupleValue(types.OptionalValue(types.Uint64Value(400))),
						),
					),
				)
				if err != nil {
					return fmt.Errorf("failed to create table: %w", err)
				}

				desc, err := session.DescribeTable(ctx, tablePath, options.WithShardKeyBounds())
				if err != nil {
					return fmt.Errorf("failed to get table description: %w", err)
				}
				if len(desc.KeyRanges) != 4 {
					return errors.New("key ranges len is not as expected")
				}

				return nil
			},
		)
		assert.NoError(t, err)
	})
}
