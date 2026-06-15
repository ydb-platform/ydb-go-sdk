//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestDatabaseSqlYSONColumnBytesValue(t *testing.T) {
	var (
		scope    = newScope(t)
		ctx      = scope.Ctx
		driver   = scope.Driver()
		expected = []byte("<a=1>[3;%false]")
	)

	tablePath := scope.TablePath(withCreateTableQueryTemplate(`
		PRAGMA TablePathPrefix("{{.TablePathPrefix}}");
		CREATE TABLE {{.TableName}} (
			id Int64 NOT NULL,
			yson Yson,
			PRIMARY KEY (id)
		)
	`))

	err := driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.BulkUpsert(ctx, tablePath, types.ListValue(
			types.StructValue(
				types.StructFieldValue("id", types.Int64Value(1)),
				types.StructFieldValue("yson", types.YSONValueFromBytes(expected)),
			),
		))
	})
	require.NoError(t, err)

	selectYSON := fmt.Sprintf("SELECT yson FROM `%s` WHERE id = 1", tablePath)

	for _, tt := range []struct {
		name         string
		queryService bool
	}{
		{
			name:         "ydb.WithQueryService(false)",
			queryService: false,
		},
		{
			name:         "ydb.WithQueryService(true)",
			queryService: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := scope.SQLDriver(ydb.WithQueryService(tt.queryService))

			for _, scanCase := range []struct {
				name string
				scan func(row *sql.Row) (any, error)
			}{
				{
					name: "BytesValue",
					scan: func(row *sql.Row) (any, error) {
						var got []byte
						err := row.Scan(&got)

						return got, err
					},
				},
				{
					name: "BytesValue_to_any",
					scan: func(row *sql.Row) (any, error) {
						var got any
						err := row.Scan(&got)

						return got, err
					},
				},
			} {
				t.Run(scanCase.name, func(t *testing.T) {
					row := db.QueryRowContext(ctx, selectYSON)

					got, err := scanCase.scan(row)
					require.NoError(t, err)
					require.Equal(t, expected, got)
				})
			}
		})
	}
}
