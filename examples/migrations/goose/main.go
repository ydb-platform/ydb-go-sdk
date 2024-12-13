// This is custom goose binary with sqlite3 support only.

package main

import (
	"context"
	"database/sql"
	"embed"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"log"
	"os"

	"github.com/pressly/goose/v3"
	_ "github.com/ydb-platform/ydb-go-sdk/v3"
)

//go:embed schema/*.sql
var embedMigrations embed.FS

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		panic(err)
	}

	defer nativeDriver.Close(ctx)

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithDefaultQueryMode(ydb.ScriptingQueryMode),
		ydb.WithFakeTx(ydb.ScriptingQueryMode),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
	)
	if err != nil {
		panic(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("ydb"); err != nil {
		panic(err)
	}

	for _, command := range []string{"status", "up", "status"} {
		if err := goose.RunContext(ctx, command, db, "schema"); err != nil {
			log.Fatalf("goose %v: %v", "", err)
		}
	}
}
