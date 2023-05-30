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
	Database() string
	ListDirectory(ctx context.Context, path string) (d scheme.Directory, err error)
}

func IsDirectoryExists(ctx context.Context, c schemeClient, directory string) (
	exists bool, _ error,
) {
	if !strings.HasPrefix(directory, c.Database()) {
		return false, xerrors.WithStackTrace(fmt.Errorf(
			"path '%s' must be inside database '%s'",
			directory, c.Database(),
		))
	}
	if directory == c.Database() {
		return true, nil
	}
	parentDirectory, childDirectory := path.Split(directory)
	parentDirectory = strings.TrimRight(parentDirectory, "/")

	if exists, err := IsDirectoryExists(ctx, c, parentDirectory); err != nil {
		return false, xerrors.WithStackTrace(err)
	} else if !exists {
		return false, nil
	}

	d, err := c.ListDirectory(ctx, parentDirectory)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}
	for i := range d.Children {
		if d.Children[i].Name != childDirectory {
			continue
		}
		if t := d.Children[i].Type; t != scheme.EntryDirectory {
			return false, xerrors.WithStackTrace(fmt.Errorf(
				"entry '%s' in path '%s' is not a directory: %s",
				childDirectory, parentDirectory, t.String(),
			))
		}
		return true, nil
	}
	return false, nil
}

func IsTableExists(ctx context.Context, c schemeClient, absTablePath string) (
	exists bool, _ error,
) {
	if !strings.HasPrefix(absTablePath, c.Database()) {
		return false, xerrors.WithStackTrace(fmt.Errorf(
			"table path '%s' must be inside database '%s'",
			absTablePath, c.Database(),
		))
	} else if absTablePath == c.Database() {
		return false, xerrors.WithStackTrace(fmt.Errorf(
			"table path '%s' cannot be equals database name '%s'",
			absTablePath, c.Database(),
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
	for i := range d.Children {
		if d.Children[i].Name != tableName {
			continue
		}
		if d.Children[i].Type != scheme.EntryTable {
			return false, xerrors.WithStackTrace(fmt.Errorf(
				"entry '%s' in path '%s' is not a table: %s",
				tableName, directory, d.Children[i].Type.String(),
			))
		}
		return true, nil
	}
	return false, nil
}
