//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestRewriteQueryArgs(t *testing.T) {
	folder := t.Name()

	ctx, cancel := context.WithTimeout(xtest.Context(t), 42*time.Second)
	defer cancel()

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	require.NoError(t, err)

	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	c, err := ydb.Connector(nativeDriver,
		ydb.WithTablePathPrefix(path.Join(nativeDriver.Name(), folder)),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
		ydb.WithRewriteQueryArgs(), // Enable query rewriting
	)
	require.NoError(t, err)

	defer func() {
		_ = c.Close()
	}()

	db := sql.OpenDB(c)
	defer func() {
		_ = db.Close()
	}()

	err = db.PingContext(ctx)
	require.NoError(t, err)

	// Clean up and create test table
	err = sugar.RemoveRecursive(ctx, nativeDriver, folder)
	require.NoError(t, err)

	err = sugar.MakeRecursive(ctx, nativeDriver, folder)
	require.NoError(t, err)

	t.Run("INClause", func(t *testing.T) {
		// Create a test table
		_, err := db.ExecContext(ctx, `
			CREATE TABLE test_table (
				id Uint64,
				name Utf8,
				PRIMARY KEY (id)
			)
		`)
		require.NoError(t, err)

		// Insert test data
		_, err = db.ExecContext(ctx, `
			INSERT INTO test_table (id, name) VALUES 
			(1, 'Alice'),
			(2, 'Bob'),
			(3, 'Charlie'),
			(4, 'David')
		`)
		require.NoError(t, err)

		// Test IN clause with multiple parameters
		// This should be transformed to IN $argsList
		rows, err := db.QueryContext(ctx, `
			SELECT id, name FROM test_table WHERE id IN ($1, $2, $3)
		`, uint64(1), uint64(2), uint64(3))
		require.NoError(t, err)

		var results []struct {
			ID   uint64
			Name string
		}

		for rows.Next() {
			var id uint64
			var name string
			err = rows.Scan(&id, &name)
			require.NoError(t, err)
			results = append(results, struct {
				ID   uint64
				Name string
			}{ID: id, Name: name})
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		// Verify we got the expected results
		require.Len(t, results, 3)
		names := make(map[string]bool)
		for _, r := range results {
			names[r.Name] = true
		}
		require.True(t, names["Alice"])
		require.True(t, names["Bob"])
		require.True(t, names["Charlie"])
	})

	t.Run("InsertValues", func(t *testing.T) {
		// Create another test table
		_, err := db.ExecContext(ctx, `
			CREATE TABLE test_insert (
				id Uint64,
				value Utf8,
				PRIMARY KEY (id)
			)
		`)
		require.NoError(t, err)

		// Test INSERT with multiple value tuples
		// This should be transformed to INSERT SELECT FROM AS_TABLE
		_, err = db.ExecContext(ctx, `
			INSERT INTO test_insert (id, value) VALUES ($1, $2), ($3, $4), ($5, $6)
		`, uint64(1), "one", uint64(2), "two", uint64(3), "three")
		require.NoError(t, err)

		// Verify the data was inserted
		rows, err := db.QueryContext(ctx, `
			SELECT id, value FROM test_insert ORDER BY id
		`)
		require.NoError(t, err)

		var results []struct {
			ID    uint64
			Value string
		}

		for rows.Next() {
			var id uint64
			var value string
			err = rows.Scan(&id, &value)
			require.NoError(t, err)
			results = append(results, struct {
				ID    uint64
				Value string
			}{ID: id, Value: value})
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		// Verify we got the expected results
		require.Len(t, results, 3)
		require.Equal(t, uint64(1), results[0].ID)
		require.Equal(t, "one", results[0].Value)
		require.Equal(t, uint64(2), results[1].ID)
		require.Equal(t, "two", results[1].Value)
		require.Equal(t, uint64(3), results[2].ID)
		require.Equal(t, "three", results[2].Value)
	})
}
