//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func BenchmarkImplicitSessions(b *testing.B) {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources

	q := db.Query()

	// Warmup
	q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)

	b.Run("implicit", func(b *testing.B) {
		for range b.N {
			q.Query(ctx, `SELECT 42 as id, "my string" as myStr`,
				query.WithImplicitSession(),
			)
		}
	})

	b.Run("explicit", func(b *testing.B) {
		for range b.N {
			q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)
		}
	})
}
