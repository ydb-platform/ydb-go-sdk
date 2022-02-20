package sugar

import (
	"context"
	"path"
	"strings"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
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
func MakeRecursive(ctx context.Context, db ydb.Connection, pathToCreate string) error {
	pathToCreate = path.Join(db.Name(), pathToCreate)
	for i := len(db.Name()) + 1; i < len(pathToCreate); i++ {
		x := strings.IndexByte(pathToCreate[i:], '/')
		if x == -1 {
			x = len(pathToCreate[i:]) - 1
		}
		i += x
		sub := pathToCreate[:i+1]
		info, err := db.Scheme().DescribePath(ctx, sub)
		var opErr *errors.OpError
		if errors.As(err, &opErr) && opErr.Reason == errors.StatusSchemeError {
			err = db.Scheme().MakeDirectory(ctx, sub)
			if err != nil {
				return errors.Errorf(0, "%w", err)
			}
			info, err = db.Scheme().DescribePath(ctx, sub)
		}
		if err != nil {
			return errors.Errorf(0, "%w", err)
		}
		switch info.Type {
		case
			scheme.EntryDatabase,
			scheme.EntryDirectory:
			// OK
		default:
			return errors.Errorf(0, "entry %q exists but it is a %s",
				sub, info.Type,
			)
		}
	}
	return nil
}

// RemoveRecursive remove selected directory or table names in database.
// pathToRemove is a database root relative path
// All database entities in prefix path will remove if names list is empty.
// Empty prefix means than use root of database.
// RemoveRecursive method equal bash command `rm -rf ~/path/to/remove`
// where `~` - is a root of database
func RemoveRecursive(ctx context.Context, db ydb.Connection, pathToRemove string) error {
	fullSysTablePath := path.Join(db.Name(), sysTable)
	var list func(int, string) error
	list = func(i int, p string) error {
		var dir scheme.Directory
		var err error
		err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			dir, err = db.Scheme().ListDirectory(ctx, p)
			return err
		}, retry.WithIdempotent())
		var opErr *errors.OpError
		if errors.As(err, &opErr) && opErr.Reason == errors.StatusSchemeError {
			return nil
		}
		if err != nil {
			return errors.Errorf(0, "%w", err)
		}

		for _, child := range dir.Children {
			pt := path.Join(p, child.Name)
			if pt == fullSysTablePath {
				continue
			}
			switch child.Type {
			case scheme.EntryDirectory:
				if err = list(i+1, pt); err != nil {
					return errors.Errorf(0, "%w", err)
				}
				err = retry.Retry(ctx, func(ctx context.Context) (err error) {
					return db.Scheme().RemoveDirectory(ctx, pt)
				}, retry.WithIdempotent())
				if err != nil {
					return errors.Errorf(0, "%w", err)
				}

			case scheme.EntryTable:
				err = db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
					return session.DropTable(ctx, pt)
				})
				if err != nil {
					return errors.Errorf(0, "%w", err)
				}

			default:
			}
		}
		return nil
	}
	return list(0, path.Join(db.Name(), pathToRemove))
}
