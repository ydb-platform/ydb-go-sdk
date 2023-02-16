package xsql

import (
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func queryLines(q string) []string {
	lines := strings.Split(q, ";")
	j := 0
	for i := range lines {
		l := strings.TrimSpace(lines[i])
		if l != "" {
			lines[j] = l
			j++
		}
	}
	return lines[:j]
}

func named(name string, value interface{}) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

func namedValues(args ...interface{}) (namedValues []driver.NamedValue) {
	for i, v := range args {
		if namedValue, has := v.(driver.NamedValue); has {
			namedValues = append(namedValues, namedValue)
		} else {
			namedValues = append(namedValues, driver.NamedValue{
				Name:    "",
				Ordinal: i,
				Value:   v,
			})
		}
	}
	return namedValues
}

func TestBindNumeric(t *testing.T) {
	for _, tt := range []struct {
		query          string
		args           []interface{}
		expectedQuery  string
		expectedParams *table.QueryParameters
		expectedErr    error
	}{
		{
			query: `
				SELECT * FROM t WHERE col = $1
					AND col2 = $2
					AND col3 = $1
					ANS col4 = $3
					AND null_coll = $4
				)
			`,
			args: []interface{}{1, uint64(2), "I'm a string param", nil},
			expectedQuery: `
				DECLARE $p1 AS Int32;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Utf8;
				DECLARE $p4 AS Void;

				SELECT * FROM t WHERE col = $p1
					AND col2 = $p2
					AND col3 = $p1
					ANS col4 = $p3
					AND null_coll = $p4
				)
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Uint64Value(2)),
				table.ValueParam("$p3", types.TextValue("I'm a string param")),
				table.ValueParam("$p4", types.VoidValue()),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $1`,
			args:  []interface{}{1},
			expectedQuery: `
				DECLARE $p1 AS Int32;

				SELECT $p1;
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $2, $1, $3`,
			args:  []interface{}{1, 2, 3},
			expectedQuery: `
				DECLARE $p1 AS Int32;
				DECLARE $p2 AS Int32;
				DECLARE $p3 AS Int32;

				SELECT $p2, $p1, $p3;
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Int32Value(2)),
				table.ValueParam("$p3", types.Int32Value(3)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $2, $1, $3`,
			args:  []interface{}{"a", "b", "c"},
			expectedQuery: `
				DECLARE $p1 AS Utf8;
				DECLARE $p2 AS Utf8;
				DECLARE $p3 AS Utf8;

				SELECT $p2, $p1, $p3;
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.TextValue("a")),
				table.ValueParam("$p2", types.TextValue("b")),
				table.ValueParam("$p3", types.TextValue("c")),
			),
			expectedErr: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			query, params, err := bind(tt.query, namedValues(tt.args...)...)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, queryLines(tt.expectedQuery), queryLines(query))
				require.Equal(t, tt.expectedParams, params)
			}
		})
	}
}

func TestBindNamed(t *testing.T) {
	for _, tt := range []struct {
		query          string
		args           []interface{}
		expectedQuery  string
		expectedParams *table.QueryParameters
		expectedErr    error
	}{
		{
			query: `
				SELECT * FROM t WHERE col = $1
					AND col2 = @p2
					AND col3 = @p1
					ANS col4 = @p3
					AND null_coll = @p4
			`,
			args: []interface{}{
				named("$p1", 1),
				named("$p2", uint64(2)),
				named("$p3", "I'm a string param"),
				named("$p4", nil),
			},
			expectedQuery:  "",
			expectedParams: nil,
			expectedErr:    errBindMixedParamsFormats,
		},
		{
			query: `
				SELECT * FROM t WHERE col = @a
					AND col2 = @b
					AND col3 = @a
					ANS col4 = @c
					AND null_coll = $d
			`,
			args: []interface{}{
				named("a", 1),
				named("b", uint64(2)),
				named("c", "I'm a string param"),
				named("d", nil),
			},
			expectedQuery: `
				DECLARE $a AS Int32;
				DECLARE $b AS Uint64;
				DECLARE $c AS Utf8;
				DECLARE $d AS Void;

				SELECT * FROM t WHERE col = $a
					AND col2 = $b
					AND col3 = $a
					ANS col4 = $c
					AND null_coll = $d
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$a", types.Int32Value(1)),
				table.ValueParam("$b", types.Uint64Value(2)),
				table.ValueParam("$c", types.TextValue("I'm a string param")),
				table.ValueParam("$d", types.VoidValue()),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT @p1`,
			args: []interface{}{
				named("p1", 1),
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;

				SELECT $p1
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT @p2, @p1, @p3`,
			args: []interface{}{
				named("p1", 1),
				named("p2", 2),
				named("p3", 3),
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;
				DECLARE $p2 AS Int32;
				DECLARE $p3 AS Int32;

				SELECT $p2, $p1, $p3
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Int32Value(2)),
				table.ValueParam("$p3", types.Int32Value(3)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT @p2, @p1, @p3`,
			args: []interface{}{
				named("p1", "a"),
				named("p2", "b"),
				named("p3", "c"),
			},
			expectedQuery: `
				DECLARE $p1 AS Utf8;
				DECLARE $p2 AS Utf8;
				DECLARE $p3 AS Utf8;

				SELECT $p2, $p1, $p3
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.TextValue("a")),
				table.ValueParam("$p2", types.TextValue("b")),
				table.ValueParam("$p3", types.TextValue("c")),
			),
			expectedErr: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			query, params, err := bind(tt.query, namedValues(tt.args...)...)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, queryLines(tt.expectedQuery), queryLines(query))
				require.Equal(t, tt.expectedParams, params)
			}
		})
	}
}

func TestBindPositional(t *testing.T) {
	for _, tt := range []struct {
		query          string
		args           []interface{}
		expectedQuery  string
		expectedParams *table.QueryParameters
		expectedErr    error
	}{
		{
			query: `
				SELECT * FROM t WHERE col = ?
					AND col2 = ?
					AND col3 = ?
					ANS col4 = ?
					AND null_coll = ?
			`,
			args: []interface{}{
				1,
				uint64(2),
				"I'm a string param",
				nil,
			},
			expectedQuery:  "",
			expectedParams: nil,
			expectedErr:    errNoPositionalArg,
		},
		{
			query: `
				SELECT * FROM t WHERE col = ?
					AND col2 = ?
					AND col3 = ?
					ANS col4 = ?
					AND null_coll = ?
			`,
			args: []interface{}{
				1,
				uint64(2),
				1,
				"I'm a string param",
				nil,
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Int32;
				DECLARE $p4 AS Utf8;
				DECLARE $p5 AS Void;

				SELECT * FROM t WHERE col = $p1
					AND col2 = $p2
					AND col3 = $p3
					ANS col4 = $p4
					AND null_coll = $p5
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Uint64Value(2)),
				table.ValueParam("$p3", types.Int32Value(1)),
				table.ValueParam("$p4", types.TextValue("I'm a string param")),
				table.ValueParam("$p5", types.VoidValue()),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT ?`,
			args: []interface{}{
				1,
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;

				SELECT $p1
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT ?, ?, ?`,
			args: []interface{}{
				1,
				2,
				3,
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;
				DECLARE $p2 AS Int32;
				DECLARE $p3 AS Int32;

				SELECT $p1, $p2, $p3
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Int32Value(2)),
				table.ValueParam("$p3", types.Int32Value(3)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT ?, ?, ?`,
			args: []interface{}{
				"a",
				"b",
				"c",
			},
			expectedQuery: `
				DECLARE $p1 AS Utf8;
				DECLARE $p2 AS Utf8;
				DECLARE $p3 AS Utf8;

				SELECT $p1, $p2, $p3
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.TextValue("a")),
				table.ValueParam("$p2", types.TextValue("b")),
				table.ValueParam("$p3", types.TextValue("c")),
			),
			expectedErr: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			query, params, err := bind(tt.query, namedValues(tt.args...)...)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, queryLines(tt.expectedQuery), queryLines(query))
				require.Equal(t, tt.expectedParams, params)
			}
		})
	}
}

func TestBindParameterOption(t *testing.T) {
	for _, tt := range []struct {
		query          string
		args           []interface{}
		expectedQuery  string
		expectedParams *table.QueryParameters
		expectedErr    error
	}{
		{
			query: `
				SELECT * FROM t WHERE col = $1
					AND col2 = @p2
					AND col3 = @p1
					ANS col4 = @p3
					AND null_coll = @p4
			`,
			args: []interface{}{
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Uint64Value(2)),
				table.ValueParam("$p3", types.TextValue("I'm a string param")),
				table.ValueParam("$p4", types.VoidValue()),
			},
			expectedQuery:  "",
			expectedParams: nil,
			expectedErr:    errBindMixedParamsFormats,
		},
		{
			query: `
				SELECT * FROM t WHERE col = $a
					AND col2 = $b
					AND col3 = $a
					ANS col4 = $c
					AND null_coll = $d
			`,
			args: []interface{}{
				table.ValueParam("$a", types.Int32Value(1)),
				table.ValueParam("$b", types.Uint64Value(2)),
				table.ValueParam("$c", types.TextValue("I'm a string param")),
				table.ValueParam("$d", types.VoidValue()),
			},
			expectedQuery: `
				DECLARE $a AS Int32;
				DECLARE $b AS Uint64;
				DECLARE $c AS Utf8;
				DECLARE $d AS Void;

				SELECT * FROM t WHERE col = $a
					AND col2 = $b
					AND col3 = $a
					ANS col4 = $c
					AND null_coll = $d
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$a", types.Int32Value(1)),
				table.ValueParam("$b", types.Uint64Value(2)),
				table.ValueParam("$c", types.TextValue("I'm a string param")),
				table.ValueParam("$d", types.VoidValue()),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $p1`,
			args: []interface{}{
				table.ValueParam("$p1", types.Int32Value(1)),
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;

				SELECT $p1
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $p2, $p1, $p3`,
			args: []interface{}{
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Int32Value(2)),
				table.ValueParam("$p3", types.Int32Value(3)),
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;
				DECLARE $p2 AS Int32;
				DECLARE $p3 AS Int32;

				SELECT $p2, $p1, $p3
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Int32Value(2)),
				table.ValueParam("$p3", types.Int32Value(3)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $p2, $p1, $p3`,
			args: []interface{}{
				table.ValueParam("$p1", types.TextValue("a")),
				table.ValueParam("$p2", types.TextValue("b")),
				table.ValueParam("$p3", types.TextValue("c")),
			},
			expectedQuery: `
				DECLARE $p1 AS Utf8;
				DECLARE $p2 AS Utf8;
				DECLARE $p3 AS Utf8;

				SELECT $p2, $p1, $p3
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.TextValue("a")),
				table.ValueParam("$p2", types.TextValue("b")),
				table.ValueParam("$p3", types.TextValue("c")),
			),
			expectedErr: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			query, params, err := bind(tt.query, namedValues(tt.args...)...)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, queryLines(tt.expectedQuery), queryLines(query))
				require.Equal(t, tt.expectedParams, params)
			}
		})
	}
}

func TestBindQueryParameters(t *testing.T) {
	for _, tt := range []struct {
		query          string
		args           []interface{}
		expectedQuery  string
		expectedParams *table.QueryParameters
		expectedErr    error
	}{
		{
			query: `
				SELECT * FROM t WHERE col = $p1
					AND col2 = $p2
					AND col3 = $p1
					ANS col4 = $p3
					AND null_coll = $p4
			`,
			args: []interface{}{
				table.NewQueryParameters(
					table.ValueParam("$p1", types.Int32Value(1)),
					table.ValueParam("$p2", types.Uint64Value(2)),
					table.ValueParam("$p3", types.TextValue("I'm a string param")),
					table.ValueParam("$p4", types.VoidValue()),
				),
				table.NewQueryParameters(
					table.ValueParam("$p1", types.Int32Value(1)),
					table.ValueParam("$p2", types.Uint64Value(2)),
					table.ValueParam("$p3", types.TextValue("I'm a string param")),
					table.ValueParam("$p4", types.VoidValue()),
				),
			},
			expectedQuery:  "",
			expectedParams: nil,
			expectedErr:    errFewQueryParametersArg,
		},
		{
			query: `
				SELECT * FROM t WHERE col = $1
					AND col2 = @p2
					AND col3 = @p1
					ANS col4 = @p3
					AND null_coll = @p4
			`,
			args: []interface{}{
				table.NewQueryParameters(
					table.ValueParam("$p1", types.Int32Value(1)),
					table.ValueParam("$p2", types.Uint64Value(2)),
					table.ValueParam("$p3", types.TextValue("I'm a string param")),
					table.ValueParam("$p4", types.VoidValue()),
				),
			},
			expectedQuery:  "",
			expectedParams: nil,
			expectedErr:    errBindMixedParamsFormats,
		},
		{
			query: `
				SELECT * FROM t WHERE col = $a
					AND col2 = $b
					AND col3 = $a
					ANS col4 = $c
					AND null_coll = $d
			`,
			args: []interface{}{
				table.NewQueryParameters(
					table.ValueParam("$a", types.Int32Value(1)),
					table.ValueParam("$b", types.Uint64Value(2)),
					table.ValueParam("$c", types.TextValue("I'm a string param")),
					table.ValueParam("$d", types.VoidValue()),
				),
			},
			expectedQuery: `
				DECLARE $a AS Int32;
				DECLARE $b AS Uint64;
				DECLARE $c AS Utf8;
				DECLARE $d AS Void;

				SELECT * FROM t WHERE col = $a
					AND col2 = $b
					AND col3 = $a
					ANS col4 = $c
					AND null_coll = $d
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$a", types.Int32Value(1)),
				table.ValueParam("$b", types.Uint64Value(2)),
				table.ValueParam("$c", types.TextValue("I'm a string param")),
				table.ValueParam("$d", types.VoidValue()),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $p1`,
			args: []interface{}{
				table.NewQueryParameters(
					table.ValueParam("$p1", types.Int32Value(1)),
				),
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;

				SELECT $p1
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $p2, $p1, $p3`,
			args: []interface{}{
				table.NewQueryParameters(
					table.ValueParam("$p1", types.Int32Value(1)),
					table.ValueParam("$p2", types.Int32Value(2)),
					table.ValueParam("$p3", types.Int32Value(3)),
				),
			},
			expectedQuery: `
				DECLARE $p1 AS Int32;
				DECLARE $p2 AS Int32;
				DECLARE $p3 AS Int32;

				SELECT $p2, $p1, $p3
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.Int32Value(1)),
				table.ValueParam("$p2", types.Int32Value(2)),
				table.ValueParam("$p3", types.Int32Value(3)),
			),
			expectedErr: nil,
		},
		{
			query: `SELECT $p2, $p1, $p3`,
			args: []interface{}{
				table.NewQueryParameters(
					table.ValueParam("$p1", types.TextValue("a")),
					table.ValueParam("$p2", types.TextValue("b")),
					table.ValueParam("$p3", types.TextValue("c")),
				),
			},
			expectedQuery: `
				DECLARE $p1 AS Utf8;
				DECLARE $p2 AS Utf8;
				DECLARE $p3 AS Utf8;

				SELECT $p2, $p1, $p3
			`,
			expectedParams: table.NewQueryParameters(
				table.ValueParam("$p1", types.TextValue("a")),
				table.ValueParam("$p2", types.TextValue("b")),
				table.ValueParam("$p3", types.TextValue("c")),
			),
			expectedErr: nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			query, params, err := bind(tt.query, namedValues(tt.args...)...)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, queryLines(tt.expectedQuery), queryLines(query))
				require.Equal(t, tt.expectedParams, params)
			}
		})
	}
}

func BenchmarkBindNumeric(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := bind(`
		SELECT * FROM t WHERE col = $1
			AND col2 = $2
			AND col3 = $1
			ANS col4 = $3
			AND null_coll = $4
		`, namedValues(1, 2, "I'm a string param", nil)...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBindPositional(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := bind(`
		SELECT * FROM t WHERE col = ?
			AND col2 = ?
			AND col3 = ?
			ANS col4 = ?
			AND null_coll = ?
		`, namedValues(1, 2, 1, "I'm a string param", nil)...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBindNamed(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := bind(`
			SELECT * FROM t WHERE col = @col1
				AND col2 = @col2
				AND col3 = @col1
				ANS col4 = @col3
				AND null_coll = @col4
			`,
			named("col1", 1),
			named("col2", 2),
			named("col3", "I'm a string param"),
			named("col4", nil),
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBindParameterOption(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := bind(`
			SELECT * FROM t WHERE col = $col1
				AND col2 = $col2
				AND col3 = $col1
				ANS col4 = $col3
				AND null_coll = $col4
			`,
			namedValues(
				table.ValueParam("$col1", types.Int32Value(1)),
				table.ValueParam("$col2", types.Uint64Value(2)),
				table.ValueParam("$col3", types.TextValue("I'm a string param")),
				table.ValueParam("$col4", types.VoidValue()),
			)...,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBindQueryParameters(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := bind(`
			SELECT * FROM t WHERE col = $col1
				AND col2 = $col2
				AND col3 = $col1
				ANS col4 = $col3
				AND null_coll = $col4
			`,
			namedValues(
				table.NewQueryParameters(
					table.ValueParam("$col1", types.Int32Value(1)),
					table.ValueParam("$col2", types.Uint64Value(2)),
					table.ValueParam("$col3", types.TextValue("I'm a string param")),
					table.ValueParam("$col4", types.VoidValue()),
				),
			)...,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}
