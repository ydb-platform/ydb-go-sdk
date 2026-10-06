//go:build integration

package integration

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestQueryWideTimezoneTypes(t *testing.T) {
	ydbVersion := os.Getenv("YDB_VERSION")
	if ydbVersion == "latest" || (ydbVersion != "nightly" && version.Lt(ydbVersion, "25.1")) {
		t.Skip("require wide timezone types")
	}

	scope := newScope(t)
	for _, tt := range []struct {
		name   string
		values []string
	}{
		{"TzDate32", []string{
			"1969-12-31,Europe/Moscow", "-144169-01-01,UTC", "148107-12-31,UTC",
		}},
		{"TzDatetime64", []string{
			"1969-12-31T12:34:56,Europe/Moscow", "-144169-01-01T00:00:00,UTC", "148107-12-31T23:59:59,UTC",
		}},
		{"TzTimestamp64", []string{
			"1969-12-31T12:34:56.123456,Europe/Moscow",
			"-144169-01-01T00:00:00,UTC", "148107-12-31T23:59:59.999999,UTC",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, text := range tt.values {
				t.Run(text, func(t *testing.T) {
					sql := fmt.Sprintf(`SELECT %s(%q) AS required,
						Just(%s(%q)) AS optional, Nothing(%s?) AS absent;`,
						tt.name, text, tt.name, text, tt.name,
					)
					t.Run("QueryRow", func(t *testing.T) {
						row, err := scope.Driver().Query().QueryRow(scope.Ctx, sql)
						require.NoError(t, err)
						assertWideTimezoneRow(t, row, tt.name, text)
					})
					t.Run("Query", func(t *testing.T) {
						result, err := scope.Driver().Query().Query(scope.Ctx, sql)
						require.NoError(t, err)
						defer func() { require.NoError(t, result.Close(scope.Ctx)) }()
						set, err := result.NextResultSet(scope.Ctx)
						require.NoError(t, err)
						assertWideTimezoneSet(t, scope, set, tt.name, text)
						_, err = result.NextResultSet(scope.Ctx)
						require.ErrorIs(t, err, io.EOF)
					})
					t.Run("QueryResultSet", func(t *testing.T) {
						set, err := scope.Driver().Query().QueryResultSet(scope.Ctx, sql)
						require.NoError(t, err)
						defer func() { require.NoError(t, set.Close(scope.Ctx)) }()
						assertWideTimezoneSet(t, scope, set, tt.name, text)
					})
				})
			}
		})
	}
}

func assertWideTimezoneSet(t *testing.T, scope *scopeT, set query.ResultSet, name, text string) {
	t.Helper()
	columnTypes := set.ColumnTypes()
	require.Len(t, columnTypes, 3)
	require.Equal(t, name, columnTypes[0].Yql())
	require.Equal(t, "Optional<"+name+">", columnTypes[1].Yql())
	require.Equal(t, "Optional<"+name+">", columnTypes[2].Yql())
	row, err := set.NextRow(scope.Ctx)
	require.NoError(t, err)
	assertWideTimezoneRow(t, row, name, text)
	_, err = set.NextRow(scope.Ctx)
	require.ErrorIs(t, err, io.EOF)
}

func assertWideTimezoneRow(t *testing.T, row query.Row, name, text string) {
	t.Helper()
	values := row.Values()
	require.Len(t, values, 3)
	require.Equal(t, name, values[0].Type().Yql())
	require.Equal(t, fmt.Sprintf("%s(%q)", name, text), values[0].Yql())
	require.Equal(t, types.OptionalValue(values[0]), values[1])
	require.Equal(t, types.NullValue(values[0].Type()), values[2])

	var required string
	var optional, absent *string
	require.NoError(t, row.Scan(&required, &optional, &absent))
	require.Equal(t, text, required)
	require.NotNil(t, optional)
	require.Equal(t, text, *optional)
	require.Nil(t, absent)

	var scanned [3]types.Value
	require.NoError(t, row.Scan(&scanned[0], &scanned[1], &scanned[2]))
	require.Equal(t, values, scanned[:])

	required = ""
	optional = nil
	absent = &required
	require.NoError(t, row.ScanNamed(
		query.Named("absent", &absent), query.Named("optional", &optional), query.Named("required", &required),
	))
	require.Equal(t, text, required)
	require.NotNil(t, optional)
	require.Equal(t, text, *optional)
	require.Nil(t, absent)

	var dst struct {
		Required string  `sql:"required"`
		Optional *string `sql:"optional"`
		Absent   *string `sql:"absent"`
	}
	dst.Absent = &required
	require.NoError(t, row.ScanStruct(&dst))
	require.Equal(t, text, dst.Required)
	require.NotNil(t, dst.Optional)
	require.Equal(t, text, *dst.Optional)
	require.Nil(t, dst.Absent)
}
