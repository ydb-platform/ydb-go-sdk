package ydbutil

import (
	"context"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/scheme"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

func EnsurePathExists(ctx context.Context, d ydb.Driver, database, path string) error {
	s := scheme.Client{
		Driver: d,
	}

	database = strings.TrimSuffix(database, "/")
	path = strings.Trim(path, "/")
	full := database + "/" + path

	for i := 0; i < len(path); i++ {
		x := strings.IndexByte(path[i:], '/')
		if x == -1 {
			x = len(path[i:])
		}
		i += x
		sub := full[:len(database)+1+i]
		info, err := s.DescribePath(ctx, sub)
		operr, ok := err.(*ydb.OpError)
		if ok && operr.Reason == ydb.StatusSchemeError {
			log.Printf("creating %q", sub)
			err = s.MakeDirectory(ctx, sub)
		}
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		log.Printf("exists %q", sub)
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

func CleanupDatabase(ctx context.Context, d ydb.Driver, sp table.SessionProvider, database string, names ...string) error {
	s := scheme.Client{
		Driver: d,
	}
	filter := make(map[string]struct{}, len(names))
	for _, n := range names {
		filter[n] = struct{}{}
	}
	var list func(int, string) error
	list = func(i int, p string) error {
		dir, err := s.ListDirectory(ctx, p)
		if err != nil {
			return err
		}
		log.Println(strings.Repeat(" ", i*2), "inspecting", dir.Name, dir.Type)
		for _, c := range dir.Children {
			if _, has := filter[c.Name]; len(filter) > 0 && !has {
				continue
			}
			pt := path.Join(p, c.Name)
			switch c.Type {
			case scheme.EntryDirectory:
				if err := list(i+1, pt); err != nil {
					return err
				}
				log.Println(strings.Repeat(" ", i*2), "removing", c.Type, pt)
				if err := s.RemoveDirectory(ctx, pt); err != nil {
					return err
				}

			case scheme.EntryTable:
				s, err := sp.Get(ctx)
				if err != nil {
					return err
				}
				log.Println(strings.Repeat(" ", i*2), "dropping", c.Type, pt)
				err = s.DropTable(ctx, pt)
				_ = sp.Put(ctx, s)
				if err != nil {
					return err
				}

			default:
				log.Println(strings.Repeat(" ", i*2), "skipping", c.Type, pt)
			}
		}
		return nil
	}
	return list(0, database)
}
