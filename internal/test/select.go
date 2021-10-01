package test

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"path"
)

func Select(ctx context.Context, db ydb.Connection) error {
	err := selectSimple(ctx, db.Table(), db.Name())
	if err != nil {
		return fmt.Errorf("select simple error: %w\n", err)
	}

	err = scanQuerySelect(ctx, db.Table(), db.Name())
	if err != nil {
		return fmt.Errorf("scan query error: %w\n", err)
	}

	err = readTable(ctx, db.Table(), path.Join(db.Name(), "series"))
	if err != nil {
		return fmt.Errorf("read table error: %w\n", err)
	}

	return nil
}
