package sugar

import (
	"context"
	"fmt"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

func IsTableExists(ctx context.Context, c scheme.Client, absTablePath string) (exists bool, _ error) {
	directory, tableName := path.Split(absTablePath)
	d, err := c.ListDirectory(ctx, directory)
	if err != nil {
		return false, err
	}
	for _, e := range d.Children {
		if e.Name != tableName {
			continue
		}
		if e.Type != scheme.EntryTable {
			return false, fmt.Errorf(
				"entry '%s' in path '%s' is not a table: %s",
				tableName, directory, e.Type.String(),
			)
		}
		return true, nil
	}
	return false, nil
}
