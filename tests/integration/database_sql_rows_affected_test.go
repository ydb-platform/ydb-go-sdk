//go:build integration
// +build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestDatabaseSQLRowsAffected(t *testing.T) {
	tests := []struct {
		sql  string
		rows int64
	}{
		{
			sql:  "INSERT INTO %s (id) values (1),(2),(3)",
			rows: 3,
		},
		{
			sql:  "UPDATE %s SET val = 'test' where id > 1",
			rows: 2,
		},
		{
			sql:  "DELETE FROM %s",
			rows: 3,
		},
		{
			sql:  "INSERT INTO %s (id) values (1),(2),(3); INSERT INTO %[1]s (id) values (4),(5); ",
			rows: 5,
		},
		{
			sql:  "UPDATE %s SET val = 'test' where id > 1; DELETE FROM %[1]s WHERE id < 4", // 4+3
			rows: 7,
		},
	}

	var (
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder(ydb.WithQueryService(true))
	)

	defer func() {
		_ = db.Close()
	}()

	for _, test := range tests {
		t.Run(test.sql, func(t *testing.T) {
			result, err := db.Exec(fmt.Sprintf(test.sql, scope.TableName()))
			require.NoError(t, err)

			got, err := result.RowsAffected()
			require.NoError(t, err)

			assert.Equal(t, test.rows, got)
		})
	}
}
