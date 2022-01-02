package sugar

import (
	"context"
	"fmt"
	"path"
	"strings"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

const (
	sysTable = ".sys"
)

// MakeRecursive creates path inside database
func MakeRecursive(ctx context.Context, db ydb.Connection, folder string) error {
	folder = path.Join(db.Name(), folder)
	for i := len(db.Name()) + 1; i < len(folder); i++ {
		x := strings.IndexByte(folder[i:], '/')
		if x == -1 {
			x = len(folder[i:]) - 1
		}
		i += x
		sub := folder[:i+1]
		info, err := db.Scheme().DescribePath(ctx, sub)
		var opErr *errors.OpError
		if errors.As(err, &opErr) && opErr.Reason == errors.StatusSchemeError {
			err = db.Scheme().MakeDirectory(ctx, sub)
			if err != nil {
				return err
			}
			info, err = db.Scheme().DescribePath(ctx, sub)
		}
		if err != nil {
			return err
		}
		switch info.Type {
		case
			scheme.EntryDatabase,
			scheme.EntryDirectory:
			// OK
		default:
			return fmt.Errorf(
				"entry %q exists but it is a %s",
				sub, info.Type,
			)
		}
	}
	return nil
}

// RemoveRecursive remove selected directory or table names in database.
// All database entities in prefix path will remove if names list is empty.
// Empty prefix means than use root of database.
// RemoveRecursive method equal bash command `rm -rf ./path/to/remove`
func RemoveRecursive(ctx context.Context, db ydb.Connection, pathToRemove string) error {
	fullSysTablePath := path.Join(db.Name(), sysTable)
	var list func(int, string) error
	list = func(i int, p string) error {
		dir, err := db.Scheme().ListDirectory(ctx, p)
		var opErr *errors.OpError
		if errors.As(err, &opErr) && opErr.Reason == errors.StatusSchemeError {
			return nil
		}
		if err != nil {
			return err
		}
		for _, child := range dir.Children {
			pt := path.Join(p, child.Name)
			if pt == fullSysTablePath {
				continue
			}
			switch child.Type {
			case scheme.EntryDirectory:
				if err = list(i+1, pt); err != nil {
					return err
				}
				if err = db.Scheme().RemoveDirectory(ctx, pt); err != nil {
					return err
				}

			case scheme.EntryTable:
				err = db.Table().Do(ctx, func(ctx context.Context, session table.Session) (err error) {
					return session.DropTable(ctx, pt)
				})
				if err != nil {
					return err
				}

			default:
			}
		}
		return nil
	}
	return list(0, path.Join(db.Name(), pathToRemove))
}
