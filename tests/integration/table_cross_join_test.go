//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestTableCrossJoin(t *testing.T) {
	var (
		ctx        = xtest.Context(t)
		scope      = newScope(t)
		db         = scope.Driver()
		table1Path = scope.TablePath(
			withTableName("table1"),
			withCreateTableOptions(
				options.WithColumn("p1", types.Optional(types.TypeText)),
				options.WithPrimaryKeyColumn("p1"),
			),
		)
		_ = scope.TablePath(
			withTableName("table2"),
			withCreateTableOptions(
				options.WithColumn("p1", types.Optional(types.TypeText)),
				options.WithPrimaryKeyColumn("p1"),
			),
		)
	)
	// upsert data into table1
	err := db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.BulkUpsert(ctx, table1Path, types.ListValue(types.StructValue(
				types.StructFieldValue("p1", types.TextValue("foo")),
			)))
		},
		table.WithIdempotent(),
	)
	scope.Require.NoError(err)

	for _, tt := range []struct {
		name       string
		subQuery   string
		withParams bool
	}{
		{
			name: "Data1FromTable1Data2FromEmptyListWithoutParams",
			subQuery: `
				$data1 = (SELECT * FROM table1);
				$data2 = Cast(AsList() As List<Struct<p2: Utf8>>);
			`,
			withParams: false,
		},
		{
			name: "Data1FromTable1Data2FromLiteralWithoutParams",
			subQuery: `
				$data1 = (SELECT * FROM table1);
				$data2 = Cast(AsList(AsStruct(CAST("t1" AS Utf8) AS p2)) As List<Struct<p2: Utf8>>);
			`,
			withParams: false,
		},
		// failed test-case
		//{
		//	name: "Data1FromTable1DeclareData2WithParams",
		//	subQuery: `
		//		DECLARE $data2 AS List<Struct<p2: Utf8>>;
		//		$data1 = (SELECT * FROM table1);
		//	`,
		//	withParams: true,
		//},
		{
			name: "Data1FromLiteralDeclareData2WithParams",
			subQuery: `
				DECLARE $data2 AS List<Struct<p2: Utf8>>;
				$data1 = (SELECT * FROM AS_TABLE(AsList(AsStruct(CAST("foo" AS Utf8?) AS p1))));
			`,
			withParams: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			query := `--!syntax_v1
				PRAGMA TablePathPrefix("` + scope.Folder() + `");
				
				/* sub-query */` + tt.subQuery + `
				/* query */
				UPSERT INTO table2
				SELECT d1.p1 AS p1,
				FROM $data1 AS d1
				CROSS JOIN AS_TABLE($data2) AS d2;
		
				SELECT COUNT(*) FROM $data1;
			`

			params := table.NewQueryParameters()
			if tt.withParams {
				params = table.NewQueryParameters(
					table.ValueParam("$data2", types.ZeroValue(types.List(types.Struct(types.StructField("p2", types.TypeUTF8))))),
				)
			}

			var got uint64
			err = db.Table().Do(ctx, func(c context.Context, s table.Session) (err error) {
				_, res, err := s.Execute(c, table.DefaultTxControl(), query, params)
				if err != nil {
					return err
				}
				defer res.Close()
				if !res.NextResultSet(ctx) {
					return fmt.Errorf("no result set")
				}
				if !res.NextRow() {
					return fmt.Errorf("no rows")
				}
				if err = res.ScanWithDefaults(&got); err != nil {
					return err
				}
				return res.Err()
			}, table.WithIdempotent())
			require.NoError(t, err, query)
			require.Equal(t, uint64(1), got, query)
		})
	}
}
