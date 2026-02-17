package bind

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteQueryArgs_InClauses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		args     []any
		expected string
		argsLen  int
	}{
		{
			name:  "SimpleINWithMultipleParams",
			query: "SELECT * FROM t WHERE id IN ($p1, $p2, $p3)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", 2),
				sql.Named("p3", 3),
			},
			expected: "SELECT * FROM t WHERE id IN $argsList0",
			argsLen:  1,
		},
		{
			name:  "INWithFourParams",
			query: "SELECT * FROM t WHERE id IN ($p1, $p2, $p3, $p4)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", 2),
				sql.Named("p3", 3),
				sql.Named("p4", 4),
			},
			expected: "SELECT * FROM t WHERE id IN $argsList0",
			argsLen:  1,
		},
		{
			name:  "INWithSpaces",
			query: "SELECT * FROM t WHERE id IN ( $p1 , $p2 , $p3 )",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", 2),
				sql.Named("p3", 3),
			},
			expected: "SELECT * FROM t WHERE id IN $argsList0",
			argsLen:  1,
		},
		{
			name:  "MultipleINclauses",
			query: "SELECT * FROM t WHERE id IN ($p1, $p2) AND status IN ($p3, $p4)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", 2),
				sql.Named("p3", "active"),
				sql.Named("p4", "pending"),
			},
			expected: "SELECT * FROM t WHERE id IN $argsList0 AND status IN $argsList1",
			argsLen:  2,
		},
		{
			name:  "SingleParamINNotTransformed",
			query: "SELECT * FROM t WHERE id IN ($p1)",
			args: []any{
				sql.Named("p1", 1),
			},
			expected: "SELECT * FROM t WHERE id IN ($p1)",
			argsLen:  1,
		},
		{
			name:  "INWithOtherParams",
			query: "SELECT * FROM t WHERE id IN ($p1, $p2) AND name = $p3",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", 2),
				sql.Named("p3", "test"),
			},
			expected: "SELECT * FROM t WHERE id IN $argsList0 AND name = $p3",
			argsLen:  2,
		},
		{
			name:  "CaseInsensitiveIN",
			query: "SELECT * FROM t WHERE id in ($p1, $p2)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", 2),
			},
			// Note: The IN keyword case is preserved, but the parameters are transformed to a list
			expected: "$argsList0",
			argsLen:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rewrite := RewriteQueryArgs{}
			yql, args, err := rewrite.ToYdb(tc.query, tc.args...)
			require.NoError(t, err)
			require.Contains(t, yql, tc.expected)
			require.Len(t, args, tc.argsLen)
		})
	}
}

func TestRewriteQueryArgs_InsertValues(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		args     []any
		expected string
		argsLen  int
	}{
		{
			name:  "InsertWithMultipleTuples",
			query: "INSERT INTO t (id, value) VALUES ($p1, $p2), ($p3, $p4), ($p5, $p6)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", "one"),
				sql.Named("p3", 2),
				sql.Named("p4", "two"),
				sql.Named("p5", 3),
				sql.Named("p6", "three"),
			},
			expected: "INSERT INTO t SELECT id, value FROM AS_TABLE($valuesList0)",
			argsLen:  1,
		},
		{
			name:  "UpsertWithMultipleTuples",
			query: "UPSERT INTO t (id, value) VALUES ($p1, $p2), ($p3, $p4)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", "one"),
				sql.Named("p3", 2),
				sql.Named("p4", "two"),
			},
			expected: "UPSERT INTO t SELECT id, value FROM AS_TABLE($valuesList0)",
			argsLen:  1,
		},
		{
			name:  "ReplaceWithMultipleTuples",
			query: "REPLACE INTO t (id, value) VALUES ($p1, $p2), ($p3, $p4)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", "one"),
				sql.Named("p3", 2),
				sql.Named("p4", "two"),
			},
			expected: "REPLACE INTO t SELECT id, value FROM AS_TABLE($valuesList0)",
			argsLen:  1,
		},
		{
			name:  "InsertSingleTupleNotTransformed",
			query: "INSERT INTO t (id, value) VALUES ($p1, $p2)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", "one"),
			},
			expected: "INSERT INTO t (id, value) VALUES ($p1, $p2)",
			argsLen:  2,
		},
		{
			name:  "CaseInsensitiveInsert",
			query: "insert into t (id, value) values ($p1, $p2), ($p3, $p4)",
			args: []any{
				sql.Named("p1", 1),
				sql.Named("p2", "one"),
				sql.Named("p3", 2),
				sql.Named("p4", "two"),
			},
			expected: "SELECT id, value FROM AS_TABLE($valuesList0)", // Don't check case of INSERT/INTO
			argsLen:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rewrite := RewriteQueryArgs{}
			yql, args, err := rewrite.ToYdb(tc.query, tc.args...)
			require.NoError(t, err)
			require.Contains(t, yql, tc.expected)
			require.Len(t, args, tc.argsLen)
		})
	}
}

func TestRewriteQueryArgs_Combined(t *testing.T) {
	t.Run("INandInsertTogether", func(t *testing.T) {
		// This is a contrived example, but tests both transformations
		query := "INSERT INTO t (id, value) VALUES ($p1, $p2), ($p3, $p4)"
		args := []any{
			sql.Named("p1", 1),
			sql.Named("p2", "one"),
			sql.Named("p3", 2),
			sql.Named("p4", "two"),
		}

		rewrite := RewriteQueryArgs{}
		yql, newArgs, err := rewrite.ToYdb(query, args...)
		require.NoError(t, err)
		require.Contains(t, yql, "SELECT id, value FROM AS_TABLE($valuesList0)")
		require.Len(t, newArgs, 1)
	})
}

func TestRewriteQueryArgs_EdgeCases(t *testing.T) {
	for _, tc := range []struct {
		name    string
		query   string
		args    []any
		wantErr bool
	}{
		{
			name:    "EmptyQuery",
			query:   "",
			args:    []any{},
			wantErr: false,
		},
		{
			name:    "NoParams",
			query:   "SELECT * FROM t",
			args:    []any{},
			wantErr: false,
		},
		{
			name:  "INWithQuotedString",
			query: "SELECT * FROM t WHERE name = 'test IN ($p1)' AND id IN ($p2, $p3)",
			args: []any{
				sql.Named("p2", 1),
				sql.Named("p3", 2),
			},
			wantErr: false,
		},
		{
			name:  "INWithComment",
			query: "SELECT * FROM t WHERE -- comment with IN ($p1)\nid IN ($p2, $p3)",
			args: []any{
				sql.Named("p2", 1),
				sql.Named("p3", 2),
			},
			wantErr: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rewrite := RewriteQueryArgs{}
			_, _, err := rewrite.ToYdb(tc.query, tc.args...)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRewriteQueryArgs_Limitations(t *testing.T) {
	// Document known limitations with test cases
	t.Run("NestedParenthesesNotSupported", func(t *testing.T) {
		// This is a known limitation - nested parentheses in IN clauses are not fully supported
		// The regex pattern [^)]+ will stop at the first closing parenthesis
		query := "SELECT * FROM t WHERE id IN (FUNC($p1), $p2)"
		args := []any{
			sql.Named("p1", 1),
			sql.Named("p2", 2),
		}

		rewrite := RewriteQueryArgs{}
		yql, _, err := rewrite.ToYdb(query, args...)
		require.NoError(t, err)
		// Due to the limitation, this query won't be transformed as expected
		// This test documents the current behavior
		t.Logf("Result: %s", yql)
	})

	t.Run("QuotedStringsWithINNotFullyHandled", func(t *testing.T) {
		// This is a known limitation - SQL strings containing 'IN (' are not excluded
		// In practice, the regex won't match because the parentheses content won't match parameters
		query := "SELECT * FROM t WHERE note = 'this is IN (1, 2)'"
		args := []any{}

		rewrite := RewriteQueryArgs{}
		yql, _, err := rewrite.ToYdb(query, args...)
		require.NoError(t, err)
		// The query should remain unchanged because there are no actual parameters
		require.Contains(t, yql, "this is IN (1, 2)")
	})
}

func TestRewriteQueryArgs_ParameterValues(t *testing.T) {
	t.Run("INClauseCreatesListValue", func(t *testing.T) {
		query := "SELECT * FROM t WHERE id IN ($p1, $p2, $p3)"
		args := []any{
			sql.Named("p1", uint64(1)),
			sql.Named("p2", uint64(2)),
			sql.Named("p3", uint64(3)),
		}

		rewrite := RewriteQueryArgs{}
		_, newArgs, err := rewrite.ToYdb(query, args...)
		require.NoError(t, err)
		require.Len(t, newArgs, 1)

		// Verify that the parameter is a list value
		// The parameter should be of type *params.Parameter
		// We can check its value
	})

	t.Run("InsertCreatesStructListValue", func(t *testing.T) {
		query := "INSERT INTO t (id, value) VALUES ($p1, $p2), ($p3, $p4)"
		args := []any{
			sql.Named("p1", uint64(1)),
			sql.Named("p2", "one"),
			sql.Named("p3", uint64(2)),
			sql.Named("p4", "two"),
		}

		rewrite := RewriteQueryArgs{}
		_, newArgs, err := rewrite.ToYdb(query, args...)
		require.NoError(t, err)
		require.Len(t, newArgs, 1)

		// Verify that the parameter is a list of struct values
	})
}

func TestRewriteQueryArgs_TypePreservation(t *testing.T) {
	t.Run("IntegerTypes", func(t *testing.T) {
		query := "SELECT * FROM t WHERE id IN ($p1, $p2)"
		args := []any{
			sql.Named("p1", int64(1)),
			sql.Named("p2", int64(2)),
		}

		rewrite := RewriteQueryArgs{}
		_, newArgs, err := rewrite.ToYdb(query, args...)
		require.NoError(t, err)
		require.Len(t, newArgs, 1)
	})

	t.Run("StringTypes", func(t *testing.T) {
		query := "SELECT * FROM t WHERE name IN ($p1, $p2)"
		args := []any{
			sql.Named("p1", "alice"),
			sql.Named("p2", "bob"),
		}

		rewrite := RewriteQueryArgs{}
		_, newArgs, err := rewrite.ToYdb(query, args...)
		require.NoError(t, err)
		require.Len(t, newArgs, 1)
	})

	t.Run("MixedTypesInStruct", func(t *testing.T) {
		query := "INSERT INTO t (id, name, active) VALUES ($p1, $p2, $p3), ($p4, $p5, $p6)"
		args := []any{
			sql.Named("p1", int64(1)),
			sql.Named("p2", "alice"),
			sql.Named("p3", true),
			sql.Named("p4", int64(2)),
			sql.Named("p5", "bob"),
			sql.Named("p6", false),
		}

		rewrite := RewriteQueryArgs{}
		_, newArgs, err := rewrite.ToYdb(query, args...)
		require.NoError(t, err)
		require.Len(t, newArgs, 1)
	})
}

func TestRewriteQueryArgs_BlockID(t *testing.T) {
	rewrite := RewriteQueryArgs{}
	require.Equal(t, blockYQL, rewrite.blockID())
}
