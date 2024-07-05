//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCreateTableDescription(sourceTest *testing.T) {
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
	for _, tt := range []struct {
		opts        []options.CreateTableOption
		description options.Description
		equal       func(t *testing.T, lhs, rhs options.Description)
	}{
		{
			opts: []options.CreateTableOption{
				options.WithColumn("a", types.TypeUint64),
				options.WithPrimaryKeyColumn("a"),
			},
			description: options.Description{
				Name: "table_0",
				Columns: []options.Column{
					{
						Name: "a",
						Type: types.TypeUint64,
					},
				},
				PrimaryKey: []string{"a"},
			},
			equal: func(t *testing.T, lhs, rhs options.Description) {
				require.Equal(t, lhs.Columns, rhs.Columns)
				require.Equal(t, lhs.PrimaryKey, rhs.PrimaryKey)
			},
		},
		{
			opts: []options.CreateTableOption{
				options.WithColumn("a", types.TypeUint64),
				options.WithColumn("b", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("a"),
				options.WithIndex("idx_b",
					options.WithIndexColumns("b"),
					options.WithIndexType(options.GlobalIndex()),
				),
			},
			description: options.Description{
				Name: "table_1",
				Columns: []options.Column{
					{
						Name: "a",
						Type: types.TypeUint64,
					},
					{
						Name: "b",
						Type: types.Optional(types.TypeUint64),
					},
				},
				PrimaryKey: []string{"a"},
				Indexes: []options.IndexDescription{
					{
						Name:         "idx_b",
						IndexColumns: []string{"b"},
						Status:       Ydb_Table.TableIndexDescription_STATUS_READY,
						Type:         options.IndexTypeGlobal,
					},
				},
			},
			equal: func(t *testing.T, lhs, rhs options.Description) {
				require.Equal(t, lhs.Columns, rhs.Columns)
				require.Equal(t, lhs.PrimaryKey, rhs.PrimaryKey)
				require.Equal(t, lhs.Indexes, rhs.Indexes)
			},
		},
		{
			opts: []options.CreateTableOption{
				options.WithColumn("a", types.TypeUint64),
				options.WithColumn("b", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("a"),
				options.WithIndex("idx_b",
					options.WithIndexColumns("b"),
					options.WithIndexType(options.GlobalAsyncIndex()),
				),
			},
			description: options.Description{
				Name: "table_2",
				Columns: []options.Column{
					{
						Name: "a",
						Type: types.TypeUint64,
					},
					{
						Name: "b",
						Type: types.Optional(types.TypeUint64),
					},
				},
				PrimaryKey: []string{"a"},
				Indexes: []options.IndexDescription{
					{
						Name:         "idx_b",
						IndexColumns: []string{"b"},
						Status:       Ydb_Table.TableIndexDescription_STATUS_READY,
						Type:         options.IndexTypeGlobalAsync,
					},
				},
			},
			equal: func(t *testing.T, lhs, rhs options.Description) {
				require.Equal(t, lhs.Columns, rhs.Columns)
				require.Equal(t, lhs.PrimaryKey, rhs.PrimaryKey)
				require.Equal(t, lhs.Indexes, rhs.Indexes)
			},
		},
	} {
		t.Run(tt.description.Name, func(t *testing.T) {
			var (
				fullTablePath = path.Join(db.Name(), "TestCreateTableDescription", tt.description.Name)
				description   options.Description
			)
			err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				var exists bool
				if exists, err = sugar.IsTableExists(ctx, db.Scheme(), fullTablePath); err != nil {
					return err
				} else if exists {
					_ = s.DropTable(ctx, fullTablePath)
				}
				err = s.CreateTable(ctx, fullTablePath, tt.opts...)
				if err != nil {
					return err
				}
				description, err = s.DescribeTable(ctx, fullTablePath)
				if err != nil {
					return err
				}
				return nil
			}, table.WithIdempotent())
			require.NoError(t, err)
			tt.equal(t, tt.description, description)
		})
	}
}
