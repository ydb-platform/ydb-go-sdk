package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func isYdbVersionHaveQueryService() error {
	minYdbVersion := strings.Split("24.1", ".")
	ydbVersion := strings.Split(os.Getenv("YDB_VERSION"), ".")
	for i, component := range ydbVersion {
		if i < len(minYdbVersion) {
			if r := strings.Compare(component, minYdbVersion[i]); r < 0 {
				return fmt.Errorf("example '%s' run on minimal YDB version '%v', but current version is '%s'",
					os.Args[0],
					strings.Join(minYdbVersion, "."),
					func() string {
						if len(ydbVersion) > 0 && ydbVersion[0] != "" {
							return strings.Join(ydbVersion, ".")
						}

						return "undefined"
					}(),
				)
			} else if r > 0 {
				return nil
			}
		}
	}

	return nil
}

func main() {
	if err := isYdbVersionHaveQueryService(); err != nil {
		fmt.Println(err.Error())

		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn, exists := os.LookupEnv("YDB_CONNECTION_STRING")
	if !exists {
		panic("YDB_CONNECTION_STRING environment variable not defined")
	}

	db, err := ydb.Open(ctx,
		dsn,
		environ.WithEnvironCredentials(ctx),
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
