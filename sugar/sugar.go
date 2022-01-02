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

// MakePath creates path inside database
func MakePath(ctx context.Context, db ydb.Connection, path string) error {
	for i := len(db.Name()) + 1; i < len(path); i++ {
		x := strings.IndexByte(path[i:], '/')
		if x == -1 {
			x = len(path[i:]) - 1
		}
		i += x
		sub := path[:i+1]
		info, err := db.Scheme().DescribePath(ctx, sub)
		var opErr *errors.OpError
		if errors.As(err, &opErr) && opErr.Reason == errors.StatusSchemeError {
			err = db.Scheme().MakeDirectory(ctx, sub)
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

// RmPath remove selected directory or table names in database.
// All database entities in prefix path will remove if names list is empty.
// Empty prefix means than use root of database.
// RmPath method equal bash command `rm -rf ./pathToRemove/{name1,name2,name3}`
func RmPath(ctx context.Context, db ydb.Connection, pathToRemove string, names ...string) error {
	filter := make(map[string]struct{}, len(names))
	for _, n := range names {
		filter[n] = struct{}{}
	}
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
			if _, has := filter[child.Name]; !has {
				continue
			}
			pt := path.Join(p, child.Name)
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
