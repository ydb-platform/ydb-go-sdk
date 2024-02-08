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

// RemoveRecursive remove selected directory or table names in database.
// pathToRemove is a database root relative path
// All database entities in prefix path will remove if names list is empty.
// Empty prefix means than use root of database.
// RemoveRecursive method equal bash command `rm -rf ~/path/to/remove`
// where `~` - is a root of database
func RemoveRecursive(ctx context.Context, db dbFoRemoveRecursive, pathToRemove string) error {
	fullSysTablePath := path.Join(db.Name(), sysDirectory)
	var removePathRecursively func(int, string) error
	removePathRecursively = rmPath(ctx, db, fullSysTablePath, removePathRecursively)
	pathToRemove = removeWithPrefix(pathToRemove, db)

	return removePathRecursively(0, pathToRemove)
}

// rmPath removes a path recursively from the database
func rmPath(
	ctx context.Context,
	db dbFoRemoveRecursive,
	fullSysTablePath string,
	removePathRecursively func(int, string) error,
) func(i int, p string) error {
	return func(i int, p string) error {
		err := checkDirectoryExists(ctx, db, p)
		if err != nil {
			return err
		}

		entry, err := db.Scheme().DescribePath(ctx, p)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("cannot describe path %q: %w", p, err),
			)
		}

		dir, err := checkEntryAndListDirectory(ctx, db, &entry, p)
		if err != nil {
			return err
		}

		for j := range dir.Children {
			pt := path.Join(p, dir.Children[j].Name)
			if pt == fullSysTablePath {
				continue
			}
			switch t := dir.Children[j].Type; t {
			case scheme.EntryDirectory:
				if err = removePathRecursively(i+1, pt); err != nil {
					return xerrors.WithStackTrace(
						fmt.Errorf("recursive removing directory %q failed: %w", pt, err),
					)
				}

			case scheme.EntryTable, scheme.EntryColumnTable:
				err = db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
					return session.DropTable(ctx, pt)
				}, table.WithIdempotent())
				if err != nil {
					return xerrors.WithStackTrace(
						fmt.Errorf("removing table %q failed: %w", pt, err),
					)
				}

			case scheme.EntryTopic:
				err = db.Topic().Drop(ctx, pt)
				if err != nil {
					return xerrors.WithStackTrace(
						fmt.Errorf("removing topic %q failed: %w", pt, err),
					)
				}

			default:
				return xerrors.WithStackTrace(
					fmt.Errorf("unknown entry type: %s", t.String()),
				)
			}
		}

		err = removeDirectoryIfNotEntry(ctx, db, &entry, p)
		if err != nil {
			return err
		}

		return nil
	}
}

// removeWithPrefix prepends the db.Name() to the pathToRemove string if it does not already have the prefix.
func removeWithPrefix(pathToRemove string, db dbFoRemoveRecursive) string {
	if !strings.HasPrefix(pathToRemove, db.Name()) {
		pathToRemove = path.Join(db.Name(), pathToRemove)
	}

	return pathToRemove
}

// checkDirectoryExists checks if a directory exists in the specified database.
func checkDirectoryExists(ctx context.Context, db dbFoRemoveRecursive, p string) error {
	exists, err := IsDirectoryExists(ctx, db.Scheme(), p)
	if err != nil {
		return xerrors.WithStackTrace(
			fmt.Errorf("check directory %q exists failed: %w", p, err),
		)
	} else if !exists {
		return nil
	}

	return nil
}

// removeDirectoryIfNotEntry removes a directory if it is not an entry.
func removeDirectoryIfNotEntry(
	ctx context.Context,
	db dbFoRemoveRecursive,
	entry *scheme.Entry,
	p string,
) error {
	if entry.Type == scheme.EntryDirectory {
		err := db.Scheme().RemoveDirectory(ctx, p)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("removing directory %q failed: %w", p, err),
			)
		}
	}

	return nil
}

// checkEntryAndListDirectory checks if the given entry is not a EntryDirectory or a EntryDatabase
// and lists its children directories and files.
func checkEntryAndListDirectory(
	ctx context.Context,
	db dbFoRemoveRecursive,
	entry *scheme.Entry,
	p string,
) (scheme.Directory, error) {
	var dir scheme.Directory
	if entry.Type != scheme.EntryDirectory && entry.Type != scheme.EntryDatabase {
		return dir, nil
	}

	dir, err := db.Scheme().ListDirectory(ctx, p)
	if err != nil {
		return dir, xerrors.WithStackTrace(
			fmt.Errorf("listing directory %q failed: %w", p, err),
		)
	}

	return dir, nil
}
