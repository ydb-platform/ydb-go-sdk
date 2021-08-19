package main

import (
	"context"
	"path"

	"github.com/YandexDatabase/ydb-go-sdk/v2"
	"github.com/YandexDatabase/ydb-go-sdk/v2/table"
)

func doCreate(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	for _, desc := range []struct {
		name string
		opts []table.CreateTableOption
	}{
		{
			name: TableSeries,
			opts: []table.CreateTableOption{
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("series_info", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("release_date", ydb.Optional(ydb.TypeDatetime)),
				table.WithColumn("views", ydb.Optional(ydb.TypeUint64)),

				table.WithPrimaryKeyColumn("series_id"),
			},
		},
		{
			name: TableSeriesRevViews,
			opts: []table.CreateTableOption{
				table.WithColumn("rev_views", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),

				table.WithPrimaryKeyColumn("rev_views", "series_id"),
			},
		},
	} {
		err := table.Retry(ctx, sp,
			table.OperationFunc(func(ctx context.Context, s *table.Session) error {
				return s.CreateTable(ctx, path.Join(prefix, desc.name), desc.opts...)
			}),
		)
		if err != nil {
			return err
		}
	}
	return nil
}
