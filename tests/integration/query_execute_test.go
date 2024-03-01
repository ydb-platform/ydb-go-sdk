//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryExecute(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	var (
		p1 string
		p2 uint64
		p3 time.Duration
	)
	err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
		_, res, err := s.Execute(ctx, `
			DECLARE $p1 AS Text;
			DECLARE $p2 AS Uint64;
			DECLARE $p3 AS Interval;
			SELECT $p1, $p2, $p3;
			`,
			query.WithParameters(
				ydb.ParamsBuilder().
					Param("$p1").Text("test").
					Param("$p2").Uint64(100500000000).
					Param("$p3").Interval(time.Duration(100500000000)).
					Build(),
			),
			query.WithSyntax(query.SyntaxYQL),
		)
		if err != nil {
			return err
		}
		rs, err := res.NextResultSet(ctx)
		if err != nil {
			return err
		}
		row, err := rs.NextRow(ctx)
		if err != nil {
			return err
		}
		err = row.Scan(&p1, &p2, &p3)
		if err != nil {
			return err
		}
		return res.Err()
	}, query.WithIdempotent())
	require.NoError(t, err)
	require.EqualValues(t, "test", p1)
	require.EqualValues(t, 100500000000, p2)
	require.EqualValues(t, time.Duration(100500000000), p3)
}
