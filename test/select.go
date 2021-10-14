// +build integration

package test

import (
	"context"
	"fmt"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func Select(ctx context.Context, db ydb.Connection) error {
	err := selectSimple(ctx, db.Table(), db.Name())
	if err != nil {
		return fmt.Errorf("select simple error: %w", err)
	}

	err = scanQuerySelect(ctx, db.Table(), db.Name())
	if err != nil {
		return fmt.Errorf("scan query error: %w", err)
	}

	err = readTable(ctx, db.Table(), path.Join(db.Name(), "series"))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	return nil
}
