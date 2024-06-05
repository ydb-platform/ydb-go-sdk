package sugar

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

const (
	sysDirectory = ".sys"
)

type dbName interface {
	Name() string
}

type dbScheme interface {
	Scheme() scheme.Client
}

type dbTable interface {
	Table() table.Client
}

type dbTopic interface {
	Topic() topic.Client
}

type dbForMakeRecursive interface {
	dbName
	dbScheme
}

type dbFoRemoveRecursive interface {
	dbName
	dbScheme
	dbTable
	dbTopic
}

// MakeRecursive creates path inside database
// pathToCreate is a database root relative path
// MakeRecursive method equal bash command `mkdir -p ~/path/to/create`
// where `~` - is a root of database
func MakeRecursive(ctx context.Context, db dbForMakeRecursive, pathToCreate string) error {
	if strings.HasPrefix(pathToCreate, sysDirectory+"/") {
		return xerrors.WithStackTrace(
			fmt.Errorf("making directory %q inside system path %q not supported", pathToCreate, sysDirectory),
		)
	}

	absPath := path.Join(db.Name(), pathToCreate)

	err := db.Scheme().MakeDirectory(ctx, absPath)
	if err != nil {
		return xerrors.WithStackTrace(
			fmt.Errorf("cannot make directory %q: %w", absPath, err),
		)
	}

	info, err := db.Scheme().DescribePath(ctx, absPath)
	if err != nil {
		return xerrors.WithStackTrace(
			fmt.Errorf("cannot describe path %q: %w", absPath, err),
		)
	}

	switch info.Type {
	case
		scheme.EntryDatabase,
		scheme.EntryDirectory:
		return nil
	default:
		return xerrors.WithStackTrace(
			fmt.Errorf("entry %q exists but it is not a directory: %s", absPath, info.Type),
		)
	}
}

// RemoveRecursive removes selected directory or table names in the database.
// pathToRemove is a database root relative path.
// All database entities in the prefix path will be removed if the names list is empty.
// An empty prefix means using the root of the database.
// RemoveRecursive method is equivalent to the bash command `rm -rf ~/path/to/remove`
// where `~` is the root of the database.
func RemoveRecursive(ctx context.Context, db dbFoRemoveRecursive, pathToRemove string) error {
	fullSysTablePath := path.Join(db.Name(), sysDirectory)

	var rmPath func(int, string) error
	rmPath = func(depth int, currentPath string) error {
		exists, err := IsDirectoryExists(ctx, db.Scheme(), currentPath)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("failed to check if directory %q exists: %w", currentPath, err),
			)
		} else if !exists {
			return nil
		}

		entry, err := db.Scheme().DescribePath(ctx, currentPath)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("cannot describe path %q: %w", currentPath, err),
			)
		}

		if entry.Type != scheme.EntryDirectory && entry.Type != scheme.EntryDatabase {
			return nil
		}

		dir, err := db.Scheme().ListDirectory(ctx, currentPath)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("failed to list directory %q: %w", currentPath, err),
			)
		}

		for i := range dir.Children {
			child := &dir.Children[i]
			childPath := path.Join(currentPath, child.Name)
			if childPath == fullSysTablePath {
				continue
			}
			if err := handleEntry(ctx, db, rmPath, depth, child, childPath); err != nil {
				return err
			}
		}

		if entry.Type == scheme.EntryDirectory {
			if err := db.Scheme().RemoveDirectory(ctx, currentPath); err != nil {
				return xerrors.WithStackTrace(
					fmt.Errorf("failed to remove directory %q: %w", currentPath, err),
				)
			}
		}

		return nil
	}

	if !strings.HasPrefix(pathToRemove, db.Name()) {
		pathToRemove = path.Join(db.Name(), pathToRemove)
	}

	return rmPath(0, pathToRemove)
}

// handleEntry processes and removes different types of database entries
func handleEntry(
	ctx context.Context,
	db dbFoRemoveRecursive,
	rmPath func(int, string) error,
	depth int,
	entry *scheme.Entry,
	entryPath string,
) error {
	switch entry.Type {
	case scheme.EntryDirectory:
		if err := rmPath(depth+1, entryPath); err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("failed to recursively remove directory %q: %w", entryPath, err),
			)
		}
	case scheme.EntryTable, scheme.EntryColumnTable:
		if err := removeTable(ctx, db, entryPath); err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("failed to remove table %q: %w", entryPath, err),
			)
		}
	case scheme.EntryTopic:
		if err := db.Topic().Drop(ctx, entryPath); err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("failed to remove topic %q: %w", entryPath, err),
			)
		}
	default:
		return xerrors.WithStackTrace(
			fmt.Errorf("unknown entry type: %s", entry.Type.String()),
		)
	}

	return nil
}

// removeTable removes a table in the database
func removeTable(ctx context.Context, db dbFoRemoveRecursive, tablePath string) error {
	return db.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		return session.DropTable(ctx, tablePath)
	}, table.WithIdempotent())
}
