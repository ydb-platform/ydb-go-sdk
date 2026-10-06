//go:build integration

package integration

import (
	"fmt"
	"os"
	"testing"
	"time"

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
						values := row.Values()
						require.Len(t, values, 3)
						require.Equal(t, tt.name, values[0].Type().Yql())
						require.Equal(t, fmt.Sprintf("%s(%q)", tt.name, text), values[0].Yql())
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
					})
				})
			}
		})
	}
}

func TestQueryWideTimezoneTime(t *testing.T) {
	ydbVersion := os.Getenv("YDB_VERSION")
	if ydbVersion == "latest" || (ydbVersion != "nightly" && version.Lt(ydbVersion, "25.1")) {
		t.Skip("require wide timezone types")
	}

	scope := newScope(t)
	moscow, err := time.LoadLocation("Europe/Moscow")
	require.NoError(t, err)
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	for _, tt := range []struct {
		name     string
		text     string
		expected time.Time
	}{
		{"TzDate32", "1969-12-31,Europe/Moscow", time.Date(1969, time.December, 31, 0, 0, 0, 0, moscow)},
		{"TzDate32", "-144169-01-01,UTC", time.Date(-144168, time.January, 1, 0, 0, 0, 0, time.UTC)},
		{"TzDate32", "148107-12-31,UTC", time.Date(148107, time.December, 31, 0, 0, 0, 0, time.UTC)},
		{"TzDate32", "-0001-02-29,UTC", time.Date(0, time.February, 29, 0, 0, 0, 0, time.UTC)},
		{"TzDate32", "10000-02-29,UTC", time.Date(10000, time.February, 29, 0, 0, 0, 0, time.UTC)},
		{"TzDate32", "2024-07-01,Europe/Berlin", time.Date(2024, time.July, 1, 0, 0, 0, 0, berlin)},
		{"TzDatetime64", "1969-12-31T12:34:56,Europe/Moscow", time.Date(1969, time.December, 31, 12, 34, 56, 0, moscow)},
		{"TzDatetime64", "-144169-01-01T00:00:00,UTC", time.Date(-144168, time.January, 1, 0, 0, 0, 0, time.UTC)},
		{"TzDatetime64", "148107-12-31T23:59:59,UTC", time.Date(148107, time.December, 31, 23, 59, 59, 0, time.UTC)},
		{"TzDatetime64", "-0001-02-29T12:34:56,UTC", time.Date(0, time.February, 29, 12, 34, 56, 0, time.UTC)},
		{"TzDatetime64", "10000-02-29T12:34:56,UTC", time.Date(10000, time.February, 29, 12, 34, 56, 0, time.UTC)},
		{"TzDatetime64", "2024-07-01T12:34:56,Europe/Berlin", time.Date(2024, time.July, 1, 12, 34, 56, 0, berlin)},
		{
			"TzTimestamp64", "1969-12-31T12:34:56.123456,Europe/Moscow",
			time.Date(1969, time.December, 31, 12, 34, 56, 123456000, moscow),
		},
		{"TzTimestamp64", "-144169-01-01T00:00:00,UTC", time.Date(-144168, time.January, 1, 0, 0, 0, 0, time.UTC)},
		{
			"TzTimestamp64", "148107-12-31T23:59:59.999999,UTC",
			time.Date(148107, time.December, 31, 23, 59, 59, 999999000, time.UTC),
		},
		{
			"TzTimestamp64", "-0001-02-29T12:34:56.123456,UTC",
			time.Date(0, time.February, 29, 12, 34, 56, 123456000, time.UTC),
		},
		{
			"TzTimestamp64", "10000-02-29T12:34:56.123456,UTC",
			time.Date(10000, time.February, 29, 12, 34, 56, 123456000, time.UTC),
		},
		{
			"TzTimestamp64", "2024-07-01T12:34:56.123456,Europe/Berlin",
			time.Date(2024, time.July, 1, 12, 34, 56, 123456000, berlin),
		},
	} {
		t.Run(tt.name+"/"+tt.text, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT %s(%q) AS required,
				Just(%s(%q)) AS optional, Nothing(%s?) AS absent;`,
				tt.name, tt.text, tt.name, tt.text, tt.name,
			)
			row, err := scope.Driver().Query().QueryRow(scope.Ctx, sql)
			require.NoError(t, err)

			var required time.Time
			var optional, absent *time.Time
			absent = &required
			require.NoError(t, row.Scan(&required, &optional, &absent))
			require.Equal(t, tt.expected, required)
			require.NotNil(t, optional)
			require.Equal(t, tt.expected, *optional)
			require.Nil(t, absent)

			required = time.Time{}
			optional = nil
			absent = &required
			require.NoError(t, row.ScanNamed(
				query.Named("absent", &absent), query.Named("optional", &optional), query.Named("required", &required),
			))
			require.Equal(t, tt.expected, required)
			require.NotNil(t, optional)
			require.Equal(t, tt.expected, *optional)
			require.Nil(t, absent)

			var dst struct {
				Required time.Time  `sql:"required"`
				Optional *time.Time `sql:"optional"`
				Absent   *time.Time `sql:"absent"`
			}
			dst.Absent = &required
			require.NoError(t, row.ScanStruct(&dst))
			require.Equal(t, tt.expected, dst.Required)
			require.NotNil(t, dst.Optional)
			require.Equal(t, tt.expected, *dst.Optional)
			require.Nil(t, dst.Absent)
		})
	}
}
