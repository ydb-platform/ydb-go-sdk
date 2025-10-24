package table

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func TestStatement_NumInput(t *testing.T) {
	t.Run("no params", func(t *testing.T) {
		s := &statement{
			params: map[string]*Ydb.Type{},
		}
		require.Equal(t, 0, s.NumInput())
	})

	t.Run("with params", func(t *testing.T) {
		s := &statement{
			params: map[string]*Ydb.Type{
				"$param1": {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
				"$param2": {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
				"$param3": {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}},
			},
		}
		require.Equal(t, 3, s.NumInput())
	})

	t.Run("nil params map", func(t *testing.T) {
		s := &statement{}
		require.Equal(t, 0, s.NumInput())
	})
}

func TestStatement_Text(t *testing.T) {
	t.Run("text query", func(t *testing.T) {
		s := &statement{
			query: queryFromText("SELECT * FROM users WHERE id = $id"),
		}
		require.Equal(t, "SELECT * FROM users WHERE id = $id", s.Text())
	})

	t.Run("prepared query", func(t *testing.T) {
		s := &statement{
			query: queryPrepared("query-id-123", "INSERT INTO users VALUES ($1, $2)"),
		}
		require.Equal(t, "INSERT INTO users VALUES ($1, $2)", s.Text())
	})

	t.Run("empty query", func(t *testing.T) {
		s := &statement{
			query: queryFromText(""),
		}
		require.Equal(t, "", s.Text())
	})
}
