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
		// Single row operations
		{
			sql:  "INSERT INTO %s (id) VALUES (6)",
			rows: 1,
		},
		{
			sql:  "UPDATE %s SET val = 'single' WHERE id = 6",
			rows: 1,
		},
		{
			sql:  "DELETE FROM %s WHERE id = 6",
			rows: 1,
		},
		// Operations affecting 0 rows
		{
			sql:  "UPDATE %s SET val = 'none' WHERE id = 999",
			rows: 0,
		},
		// More complex multi-statement scenarios
		{
			sql:  "INSERT INTO %s (id) VALUES (10),(11),(12); UPDATE %[1]s SET val = 'multi' WHERE id IN (10,11,12); DELETE FROM %[1]s WHERE id = 10",
			rows: 7, // 3 inserted + 3 updated + 1 deleted
		},
		// Multiple statements with some affecting 0 rows
		{
			sql:  "UPDATE %s SET val = 'zero' WHERE id = 999; INSERT INTO %[1]s (id) VALUES (13)",
			rows: 1, // 0 updated + 1 inserted
		},
		// UPSERT operations
		{
			sql:  "UPSERT INTO %s (id, val) VALUES (14, 'upsert1')",
			rows: 1,
		},
		{
			sql:  "UPSERT INTO %s (id, val) VALUES (15, 'upsert2'), (16, 'upsert3')",
			rows: 2,
		},
		// UPSERT that updates existing rows (should still count as affecting rows)
		{
			sql:  "UPSERT INTO %s (id, val) VALUES (1, 'updated')",
			rows: 1,
		},
		// Complex multi-statement with UPSERT
		{
			sql:  "INSERT INTO %s (id, val) VALUES (17, 'insert'); UPSERT INTO %[1]s (id, val) VALUES (17, 'upserted')",
			rows: 2, // 1 inserted + 1 upserted
		},
		// Test case with mixed operations
		{
			sql:  "INSERT INTO %s (id, val) VALUES (18, 'mixed1'); UPDATE %[1]s SET val = 'updated' WHERE id = 18; DELETE FROM %[1]s WHERE id = 18",
			rows: 3, // 1 inserted + 1 updated + 1 deleted
		},
		// Additional test cases
		{
			sql:  "INSERT INTO %s (id, val) VALUES (19, 'test1'), (20, 'test2'); DELETE FROM %[1]s WHERE id IN (19, 20)",
			rows: 4, // 2 inserted + 2 deleted
		},
		{
			sql:  "UPDATE %s SET val = 'updated' WHERE id BETWEEN 1 AND 3",
			rows: 1,
		},
		{
			sql:  "INSERT INTO %s (id, val) VALUES (21, 'test1'); INSERT INTO %[1]s (id, val) VALUES (22, 'test2'); UPDATE %[1]s SET val = 'bulk_update' WHERE id IN (21, 22)",
			rows: 4, // 2 inserted + 2 updated
		},
		{
			sql:  "UPSERT INTO %s (id, val) VALUES (23, 'upsert1'); UPSERT INTO %[1]s (id, val) VALUES (24, 'upsert2'); DELETE FROM %[1]s WHERE id IN (23, 24)",
			rows: 4, // 2 upserted + 2 deleted
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
		sql := fmt.Sprintf(test.sql, scope.TableName())
		t.Run(sql, func(t *testing.T) {
			result, err := db.Exec(sql)
			require.NoError(t, err)

			got, err := result.RowsAffected()
			require.NoError(t, err)

			assert.Equal(t, test.rows, got)
		})
	}
}
