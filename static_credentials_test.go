//go:build !fast
// +build !fast

package ydb_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	user     = "test"
	password = "test"
)

func TestStaticCredentials(t *testing.T) {
	ctx := context.Background()

	t.Run("create user", func(t *testing.T) {
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

		createUserQuery := fmt.Sprintf("CREATE USER %s PASSWORD %q", user, password)

		_, err = db.ExecContext(ydb.WithQueryMode(ctx, ydb.ScriptingQueryMode), createUserQuery)
		if err != nil {
			t.Fatal(err)
		}

		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("select under new user", func(t *testing.T) {
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

		row := db.QueryRowContext(ctx, "SELECT 1")
		var result int
		if err = row.Scan(&result); err != nil {
			t.Fatal(err)
		}
		if err = row.Err(); err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 1, result)
	})
	t.Run("drop user", func(t *testing.T) {
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

		dropUserQuery := fmt.Sprintf("DROP USER %s", user)

		_, err = db.ExecContext(ydb.WithQueryMode(ctx, ydb.ScriptingQueryMode), dropUserQuery)
		if err != nil {
			t.Fatal(err)
		}

		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
