package ydb_test

import (
	"context"
	"log"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func Example_tableCreateTable() {
	ctx := context.Background()
	db, err := ydb.Open(ctx,
		"grpcs://localhost:2135/?database=/local",
		ydb.WithAnonymousCredentials(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()
	err = db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(db.Name(), "series"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeDate)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
	)
	if err != nil {
		log.Printf("unexpected error: %v", err)
	}
}
