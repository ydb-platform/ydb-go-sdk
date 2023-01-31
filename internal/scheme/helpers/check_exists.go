package helpers

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type schemeClient interface {
	ListDirectory(ctx context.Context, path string) (d scheme.Directory, err error)
}

func databaseName(c schemeClient) string {
	if name, has := c.(interface {
		Database() string
	}); has {
		return name.Database()
	}
	return "/"
}

func IsDirectoryExists(ctx context.Context, c schemeClient, directory string) (exists bool, _ error) {
	parentDirectory, childDirectory := path.Split(directory)
	parentDirectory = strings.TrimRight(parentDirectory, "/")
	if parentDirectory != databaseName(c) {
		if exists, err := IsDirectoryExists(ctx, c, parentDirectory); err != nil {
			return false, xerrors.WithStackTrace(err)
		} else if !exists {
			return false, nil
		}
	}
	d, err := c.ListDirectory(ctx, parentDirectory)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}
	for _, e := range d.Children {
		if e.Name != childDirectory {
			continue
		}
		if e.Type != scheme.EntryDirectory {
			return false, xerrors.WithStackTrace(fmt.Errorf(
				"entry '%s' in path '%s' is not a direectory: %s",
				childDirectory, parentDirectory, e.Type.String(),
			))
		}
		return true, nil
	}
	return false, nil
}

func IsTableExists(ctx context.Context, c schemeClient, absTablePath string) (exists bool, _ error) {
	if !strings.HasPrefix(absTablePath, databaseName(c)) {
		return false, xerrors.WithStackTrace(fmt.Errorf(
			"table path '%s' must be inside database '%s'",
			absTablePath, databaseName(c),
		))
	} else if absTablePath == databaseName(c) {
		return false, xerrors.WithStackTrace(fmt.Errorf(
			"table path '%s' cannot be equals database name '%s'",
			absTablePath, databaseName(c),
		))
	}
	directory, tableName := path.Split(absTablePath)
	if exists, err := IsDirectoryExists(ctx, c, strings.TrimRight(directory, "/")); err != nil {
		return false, xerrors.WithStackTrace(err)
	} else if !exists {
		return false, nil
	}
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
