package table

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

func TestTextQuery(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		q := textQuery("SELECT 1")
		require.Equal(t, "SELECT 1", q.String())
	})

	t.Run("ID", func(t *testing.T) {
		q := textQuery("SELECT 1")
		require.Equal(t, "", q.ID())
	})

	t.Run("YQL", func(t *testing.T) {
		q := textQuery("SELECT 1")
		require.Equal(t, "SELECT 1", q.YQL())
	})

	t.Run("toYDB", func(t *testing.T) {
		q := textQuery("SELECT * FROM users")
		ydbQuery := q.toYDB()
		require.NotNil(t, ydbQuery)
		require.NotNil(t, ydbQuery.Query)
		require.IsType(t, &Ydb_Table.Query_YqlText{}, ydbQuery.Query)
		require.NotNil(t, ydbQuery.GetYqlText())
		require.Equal(t, "SELECT * FROM users", ydbQuery.GetYqlText())
	})
}

func TestPreparedQuery(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		q := preparedQuery{id: "query-id-123", sql: "SELECT 1"}
		require.Equal(t, "SELECT 1", q.String())
	})

	t.Run("ID", func(t *testing.T) {
		q := preparedQuery{id: "query-id-123", sql: "SELECT 1"}
		require.Equal(t, "query-id-123", q.ID())
	})

	t.Run("YQL", func(t *testing.T) {
		q := preparedQuery{id: "query-id-123", sql: "SELECT 1"}
		require.Equal(t, "SELECT 1", q.YQL())
	})

	t.Run("toYDB", func(t *testing.T) {
		q := preparedQuery{id: "query-id-456", sql: "INSERT INTO users VALUES ($1, $2)"}
		ydbQuery := q.toYDB()
		require.NotNil(t, ydbQuery)
		require.NotNil(t, ydbQuery.Query)
		require.IsType(t, &Ydb_Table.Query_YqlText{}, ydbQuery.Query)
		require.NotNil(t, ydbQuery.GetYqlText())
		require.Equal(t, "INSERT INTO users VALUES ($1, $2)", ydbQuery.GetYqlText())
	})
}

func TestQueryFromText(t *testing.T) {
	t.Run("creates text query", func(t *testing.T) {
		q := queryFromText("SELECT 1")
		require.NotNil(t, q)
		require.Equal(t, "SELECT 1", q.String())
		require.Equal(t, "", q.ID())
		require.Equal(t, "SELECT 1", q.YQL())
	})

	t.Run("empty query", func(t *testing.T) {
		q := queryFromText("")
		require.NotNil(t, q)
		require.Equal(t, "", q.String())
		require.Equal(t, "", q.ID())
	})
}

func TestQueryPrepared(t *testing.T) {
	t.Run("creates prepared query", func(t *testing.T) {
		q := queryPrepared("query-123", "SELECT * FROM table")
		require.NotNil(t, q)
		require.Equal(t, "SELECT * FROM table", q.String())
		require.Equal(t, "query-123", q.ID())
		require.Equal(t, "SELECT * FROM table", q.YQL())
	})

	t.Run("empty id and sql", func(t *testing.T) {
		q := queryPrepared("", "")
		require.NotNil(t, q)
		require.Equal(t, "", q.String())
		require.Equal(t, "", q.ID())
	})
}

func TestQueryInterface(t *testing.T) {
	t.Run("text query implements Query interface", func(t *testing.T) {
		var q Query = textQuery("SELECT 1")
		require.NotNil(t, q)
		require.Equal(t, "SELECT 1", q.String())
		require.Equal(t, "", q.ID())
		require.Equal(t, "SELECT 1", q.YQL())
		ydbQuery := q.toYDB()
		require.NotNil(t, ydbQuery)
		require.NotNil(t, ydbQuery.Query)
		require.IsType(t, &Ydb_Table.Query_YqlText{}, ydbQuery.Query)
		yqlText, ok := ydbQuery.Query.(*Ydb_Table.Query_YqlText)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", yqlText.YqlText)
		require.Equal(t, "SELECT 1", ydbQuery.GetYqlText())
	})

	t.Run("prepared query implements Query interface", func(t *testing.T) {
		var q Query = preparedQuery{id: "test-id", sql: "SELECT 2"}
		require.NotNil(t, q)
		require.Equal(t, "SELECT 2", q.String())
		require.Equal(t, "test-id", q.ID())
		require.Equal(t, "SELECT 2", q.YQL())
		ydbQuery := q.toYDB()
		require.NotNil(t, ydbQuery)
		require.NotNil(t, ydbQuery.Query)
		require.IsType(t, &Ydb_Table.Query_YqlText{}, ydbQuery.Query)
		require.Equal(t, "SELECT 2", ydbQuery.GetYqlText())
	})
}
