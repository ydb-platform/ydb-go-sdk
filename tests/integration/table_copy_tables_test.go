//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestCopyTables(t *testing.T) {
	var (
		ctx        = xtest.Context(t)
		scope      = newScope(t)
		db         = scope.Driver()
		fromPath   = scope.TablePath()
		dirPath, _ = path.Split(fromPath)
		toPath     = path.Join(dirPath, "renamed")
	)
	// copy tables
	err := db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CopyTables(ctx,
				options.CopyTablesItem(
					fromPath,
					toPath,
					true,
				),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		t.Fatal(err)
	}
	// describe table in destination path
	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			d, err := s.DescribeTable(ctx, toPath)
			if err != nil {
				return err
			}
			fmt.Printf("Table `%s`:\n", toPath)
			for _, c := range d.Columns {
				fmt.Printf(" - `%s` %s\n", c.Name, c.Type.Yql())
			}
			return nil
		},
		table.WithIdempotent(),
	)
	if err != nil {
		t.Fatal(err)
	}
}
