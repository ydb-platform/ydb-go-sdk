//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestDatabaseSQLDefaultProcessor(st *testing.T) {
	t := newScope(st)

	db, err := sql.Open("ydb", t.ConnectionString())
	t.Require.NoError(err)
	defer db.Close()

	_, err = db.Exec("DISCARD SELECT 1")

	if os.Getenv("YDB_VERSION") == "nightly" {
		t.Require.NoError(err)
	} else {
		t.Require.Error(err, "DISCARD is supported in TABLE service but not in QUERY service")
	}
}
