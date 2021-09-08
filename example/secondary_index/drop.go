package main

import (
	"context"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func doDrop(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	for _, name := range []string{TableSeries, TableSeriesRevViews} {
		err := table.Retry(ctx, sp,
			table.OperationFunc(func(ctx context.Context, s *table.Session) error {
				return s.DropTable(ctx, path.Join(prefix, name))
			}),
		)
		if err != nil {
			return err
		}
	}
	return nil
}
