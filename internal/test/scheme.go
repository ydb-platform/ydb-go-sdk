package test

import (
	"context"
	"path"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func Prepare(ctx context.Context, t *testing.T, db ydb.Connection) {
	err := db.Scheme().CleanupDatabase(ctx, db.Name(), "series", "episodes", "seasons")
	if err != nil {
		t.Fatalf("cleaunup database failed: %v\n", err)
	}

	err = db.Scheme().EnsurePathExists(ctx, db.Name())
	if err != nil {
		t.Fatalf("ensure path exists failed: %v\n", err)
	}

	err = describeTableOptions(ctx, db.Table())
	if err != nil {
		t.Fatalf("describe table options error: %v\n", err)
	}

	err = createTables(ctx, db.Table(), db.Name())
	if err != nil {
		t.Fatalf("create tables error: %v\n", err)
	}

	err = describeTable(ctx, db.Table(), path.Join(db.Name(), "series"))
	if err != nil {
		t.Fatalf("describe table error: %v\n", err)
	}
}
