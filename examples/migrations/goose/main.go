// This is custom goose binary with sqlite3 support only.

package main

import (
	"context"
	"database/sql"
	"embed"
	"log"
	"net/url"
	"os"

	"github.com/pressly/goose/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

//go:embed schema/*.sql
var embedMigrations embed.FS

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connectionString := os.Getenv("YDB_CONNECTION_STRING")

	nativeDriver, err := ydb.Open(ctx, connectionString)
	if err != nil {
		panic(err)
	}

	defer nativeDriver.Close(ctx)

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithDefaultQueryMode(ydb.QueryExecuteQueryMode),
		ydb.WithFakeTx(ydb.QueryExecuteQueryMode),
		//ydb.WithQueryService(false),
		//ydb.WithDefaultQueryMode(ydb.ScriptingQueryMode),
		//ydb.WithFakeTx(ydb.ScriptingQueryMode),
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

	dsn := func() string {
		dsn, err := url.Parse(connectionString)
		if err != nil {
			panic(err)
		}

		q := dsn.Query()
		q.Set("go_query_mode", "query")
		q.Set("go_fake_tx", "query")
		q.Set("go_query_bind", "declare")
		q.Add("go_query_bind", "numeric")
		dsn.RawQuery = q.Encode()

		return dsn.String()
	}()

	for _, command := range []string{
		"version",
		"up-by-one",
		"status",
		"up",
		"status",
		"version",
		"down",
		"version",
		"down",
		"version",
		"reset",
		"version",
		"status",
	} {
		log.Printf("try to run command `goose ydb \"%s\" %s`...", dsn, command)
		if err := goose.RunContext(ctx, command, db, "schema"); err != nil {
			log.Fatalf("goose %v: %v", command, err)
		}
	}
}
