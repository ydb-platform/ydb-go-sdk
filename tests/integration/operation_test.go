//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestOperationList(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
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

	var nextToken string
	for {
		operations, err := db.Operation().List(ctx, operation.KindBuildIndex,
			operation.WithPageSize(10),
			operation.WithPageToken(nextToken),
		)
		require.NoError(t, err)
		nextToken = operations.NextToken

		for _, op := range operations.Operations {
			t.Log(op)
		}

		if len(operations.Operations) == 0 || nextToken == "" {
			break
		}
	}
}
