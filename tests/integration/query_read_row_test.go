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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryReadRow(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.WithSessionPoolSizeLimit(10),
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

	row, err := db.Query().ReadRow(ctx, `
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
	require.NoError(t, err)

	var (
		p1 string
		p2 uint64
		p3 time.Duration
	)
	err = row.Scan(&p1, &p2, &p3)
	require.NoError(t, err)
	require.EqualValues(t, "test", p1)
	require.EqualValues(t, 100500000000, p2)
	require.EqualValues(t, time.Duration(100500000000), p3)
}
