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
	var (
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder(ydb.WithQueryService(true))
	)

	defer func() {
		_ = db.Close()
	}()

	result, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id) values (1),(2),(3) ", scope.TableName()))
	require.NoError(t, err)

	got, err := result.RowsAffected()
	require.NoError(t, err)

	assert.Equal(t, int64(3), got)
}
