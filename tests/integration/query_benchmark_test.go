//go:build integration && (darwin || linux)

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/tests/integration/internal/querybench"
)

func BenchmarkQueryRows(b *testing.B) {
	ctx, db := querybench.Setup(b, newScope(b).ConnectionString())
	if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		for _, size := range []int{1, 10, 100, 1000, 10000} {
			sql := querybench.SQL(size)
			expected := querybench.VerifyRows(ctx, b, s, sql, size)
			b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
				querybench.Measure(b, size, expected, func() (int, uint64, error) {
					return querybench.ConsumeRows(ctx, s, sql, nil)
				})
			})
		}

		return nil
	}); err != nil {
		b.Fatal(err)
	}
}
