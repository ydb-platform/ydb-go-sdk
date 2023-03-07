package sugar

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

const (
	sysTable = ".sys"
)

// MakeRecursive creates path inside database
// pathToCreate is a database root relative path
// MakeRecursive method equal bash command `mkdir -p ~/path/to/create`
// where `~` - is a root of database
func MakeRecursive(ctx context.Context, db *ydb.Driver, pathToCreate string) error {
	if strings.HasPrefix(pathToCreate, sysTable+"/") {
		return xerrors.WithStackTrace(
			fmt.Errorf("making directory %q inside system path %q not supported", pathToCreate, sysTable),
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
func RemoveRecursive(ctx context.Context, db *ydb.Driver, pathToRemove string) error {
	fullSysTablePath := path.Join(db.Name(), sysTable)
	var rmPath func(int, string) error
	rmPath = func(i int, p string) error {
		if exists, err := IsDirectoryExists(ctx, db.Scheme(), p); err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("check directory %q exists failed: %w", p, err),
			)
		} else if !exists {
			return nil
		}

		entry, err := db.Scheme().DescribePath(ctx, p)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("cannot describe path %q: %w", p, err),
			)
		}

		if entry.Type != scheme.EntryDirectory {
			return nil
		}

		dir, err := db.Scheme().ListDirectory(ctx, p)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("listing directory %q failed: %w", p, err),
			)
		}

		for _, child := range dir.Children {
			pt := path.Join(p, child.Name)
			if pt == fullSysTablePath {
				continue
			}
			switch child.Type {
			case scheme.EntryDirectory:
				if err = rmPath(i+1, pt); err != nil {
					return xerrors.WithStackTrace(
						fmt.Errorf("recursive removing directory %q failed: %w", pt, err),
					)
				}

			case scheme.EntryTable:
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
					fmt.Errorf("unknown entry type: %s", child.Type.String()),
				)
			}
		}

		err = db.Scheme().RemoveDirectory(ctx, p)
		if err != nil {
			return xerrors.WithStackTrace(
				fmt.Errorf("removing directory %q failed: %w", p, err),
			)
		}

		return nil
	}
	if !strings.HasPrefix(pathToRemove, db.Name()) {
		pathToRemove = path.Join(db.Name(), pathToRemove)
	}
	return rmPath(0, pathToRemove)
}
