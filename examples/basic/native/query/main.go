package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func isYdbVersionHaveQueryService() error {
	minYdbVersion := strings.Split("24.1", ".")
	ydbVersion, has := os.LookupEnv("YDB_VERSION")
	if !has {
		return nil
	}
	versionComponents := strings.Split(ydbVersion, ".")
	for i, component := range versionComponents {
		if i < len(minYdbVersion) {
			if r := strings.Compare(component, minYdbVersion[i]); r < 0 {
				return fmt.Errorf("example '%s' run on minimal YDB version '%v', but current version is '%s'",
					os.Args[0],
					strings.Join(minYdbVersion, "."),
					ydbVersion,
				)
			} else if r > 0 {
				return nil
			}
		}
	}

	return nil
}

var connectionString = flag.String("ydb", os.Getenv("YDB_CONNECTION_STRING"), "")

func main() {
	if err := isYdbVersionHaveQueryService(); err != nil {
		fmt.Println(err.Error())

		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flag.Parse()

	db, err := ydb.Open(ctx, *connectionString,
		environ.WithEnvironCredentials(),
	)
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	prefix := path.Join(db.Name(), "native/query")

	err = sugar.RemoveRecursive(ctx, db, prefix)
	if err != nil {
		panic(err)
	}

	err = createTables(ctx, db.Query(), prefix)
	if err != nil {
		panic(fmt.Errorf("create tables error: %w", err))
	}

	err = fillTablesWithData(ctx, db.Query(), prefix)
	if err != nil {
		panic(fmt.Errorf("fill tables with data error: %w", err))
	}

	err = read(ctx, db.Query(), prefix)
	if err != nil {
		panic(fmt.Errorf("select simple error: %w", err))
	}
}
