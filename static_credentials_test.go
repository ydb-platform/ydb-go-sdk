//go:build !fast
// +build !fast

package ydb_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	user     = "testUser"
	password = "testPassword"
)

func TestStaticCredentials(t *testing.T) {
	ctx := context.Background()

	t.Run("sql.Open", func(t *testing.T) {
		db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
		if err != nil {
			t.Fatal(err)
		}

		if err = db.PingContext(ctx); err != nil {
			t.Fatalf("driver not initialized: %+v", err)
		}

		_, err = ydb.Unwrap(db)
		if err != nil {
			t.Fatal(err)
		}

		createUserQuery := fmt.Sprintf("CREATE USER '%s' PASSWORD '%s';", user, password)

		_, err = db.ExecContext(ydb.WithQueryMode(ctx, ydb.ScriptingQueryMode), createUserQuery)
		if err != nil {
			t.Fatal(err)
		}

		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("sql.OpenDB", func(t *testing.T) {
		u, err := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword(user, password)

		db, err := sql.Open("ydb", u.String())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			// cleanup
			_ = db.Close()
		}()

		if err = db.PingContext(ctx); err != nil {
			t.Fatal(err)
		}

		_, err = db.ExecContext(
			ydb.WithQueryMode(ctx, ydb.ScriptingQueryMode),
			fmt.Sprintf("DROP USER %s;", user),
		)
		if err != nil {
			t.Fatal(err)
		}
	})
}
