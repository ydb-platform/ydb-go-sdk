//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryTxExecute(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.WithTraceQuery(
			log.Query(
				log.Default(os.Stdout,
					log.WithLogQuery(),
					log.WithColoring(),
					log.WithMinLevel(log.INFO),
				),
				trace.QueryEvents,
			),
		),
	)
	require.NoError(t, err)
	var (
		columnNames []string
		columnTypes []string
	)
	err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) (err error) {
		res, err := tx.Execute(ctx, "SELECT 1 AS col1")
		if err != nil {
			return err
		}
		rs, err := res.NextResultSet(ctx)
		if err != nil {
			return err
		}
		columnNames = rs.Columns()
		for _, t := range rs.ColumnTypes() {
			columnTypes = append(columnTypes, t.Yql())
		}
		row, err := rs.NextRow(ctx)
		if err != nil {
			return err
		}
		var col1 int
		err = row.ScanNamed(query.Named("col1", &col1))
		if err != nil {
			return err
		}
		return res.Err()
	}, query.WithIdempotent(), query.WithTxSettings(query.TxSettings(query.WithSerializableReadWrite())))
	require.NoError(t, err)
	require.Equal(t, []string{"col1"}, columnNames)
	require.Equal(t, []string{"Int32"}, columnTypes)
}
