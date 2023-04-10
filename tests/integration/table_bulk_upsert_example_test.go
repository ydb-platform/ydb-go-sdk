//go:build !fast
// +build !fast

package integration

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestTableBulkUpsertExample(t *testing.T) {
	scope := newScope(t)
	driver := scope.Driver(
		ydb.With(config.WithGrpcOptions(
			grpc.WithDefaultCallOptions(
				grpc.UseCompressor(gzip.Name),
			),
		)),
	)
	tablePath := scope.TablePath()

	// upsert
	var rows []types.Value

	for i := int64(0); i < 10; i++ {
		val := fmt.Sprintf("value for %v", i)
		rows = append(rows, types.StructValue(
			types.StructFieldValue("id", types.Int64Value(i)),
			types.StructFieldValue("val", types.TextValue(val)),
		))
	}

	err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		return s.BulkUpsert(ctx, tablePath, types.ListValue(rows...))
	})
	scope.Require.NoError(err)
}
