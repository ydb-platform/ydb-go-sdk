//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func BenchmarkQuery_Query_WithImplicitSession(b *testing.B) {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources

	q := db.Query()

	// Warmup
	_, err = q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)
	require.NoError(b, err)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := q.Query(ctx, `SELECT 42 as id, "my string" as myStr`,
				query.WithImplicitSession(),
			)
			require.NoError(b, err)
		}
	})
}

func BenchmarkQuery_Query(b *testing.B) {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources

	q := db.Query()

	// Warmup
	_, err = q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)
	require.NoError(b, err)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)
			require.NoError(b, err)
		}
	})
}
