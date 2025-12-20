//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

// TestDatabaseSqlExtendedTypes tests working with extended date/interval types
// (Date32, Datetime64, Timestamp64, Interval64) through database/sql driver with QueryService.
// Extended types are only supported with ydb.WithQueryService(true)
func TestDatabaseSqlExtendedTypes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nativeDriver, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithQueryService(true),
		ydb.WithAutoDeclare(),
	)
	require.NoError(t, err)
	defer func() {
		_ = connector.Close()
	}()

	db := sql.OpenDB(connector)
	defer func() {
		_ = db.Close()
	}()

	t.Run("Date32", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Date32), CAST($p AS Text)`, sql.Named("p",
			time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC).Format("2006-01-02"),
			dst.UTC().Format("2006-01-02"),
		)
		require.Equal(t, "2024-06-15T00:00:00Z", s)
	})

	t.Run("Date32Negative", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Date32), CAST($p AS Text)`, sql.Named("p",
			time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02"),
			dst.UTC().Format("2006-01-02"),
		)
		require.Equal(t, "1950-01-01T00:00:00Z", s)
	})

	t.Run("Datetime64", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Datetime64), CAST($p AS Text)`, sql.Named("p",
			time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC).Unix(),
			dst.UTC().Unix(),
		)
		require.Equal(t, "2024-06-15T14:30:45Z", s)
	})

	t.Run("Datetime64Negative", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Datetime64), CAST($p AS Text)`, sql.Named("p",
			time.Date(1960, 3, 20, 10, 15, 30, 0, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(1960, 3, 20, 10, 15, 30, 0, time.UTC).Unix(),
			dst.UTC().Unix(),
		)
		require.Equal(t, "1960-03-20T10:15:30Z", s)
	})

	t.Run("Timestamp64", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Timestamp64), CAST($p AS Text)`, sql.Named("p",
			time.Date(2024, 6, 15, 14, 30, 45, 123456000, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(2024, 6, 15, 14, 30, 45, 123456000, time.UTC).Unix(),
			dst.UTC().Unix(),
		)
		require.Equal(t, 123456000/1000, dst.UTC().Nanosecond()/1000)
		require.Equal(t, "2024-06-15T14:30:45.123456Z", s)
	})

	t.Run("Timestamp64Negative", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Timestamp64), CAST($p AS Text)`, sql.Named("p",
			time.Date(1965, 7, 10, 8, 20, 15, 500000000, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(1965, 7, 10, 8, 20, 15, 500000000, time.UTC).Unix(),
			dst.UTC().Unix(),
		)
		require.Equal(t, 500000000/1000, dst.UTC().Nanosecond()/1000)
		require.Equal(t, "1965-07-10T08:20:15.500000Z", s)
	})

	t.Run("Interval64", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Interval64), CAST($p AS Text)`, sql.Named("p",
			5*time.Hour+30*time.Minute+45*time.Second+123*time.Microsecond,
		))
		require.NoError(t, row.Err())

		var (
			dst time.Duration
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t, 5*time.Hour+30*time.Minute+45*time.Second+123*time.Microsecond, dst)
		require.Equal(t, "PT5H30M45.000123S", s)
	})

	t.Run("Interval64Negative", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Interval64), CAST($p AS Text)`, sql.Named("p",
			-(2*time.Hour+15*time.Minute+30*time.Second),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Duration
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t, -(2*time.Hour + 15*time.Minute + 30*time.Second), dst)
		require.Equal(t, "-PT2H15M30S", s)
	})

	t.Run("Date32RoundTrip", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT $p, CAST($p AS Text)`, sql.Named("p",
			time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC).Format("2006-01-02"),
			dst.UTC().Format("2006-01-02"),
		)
		require.Equal(t, "2024-12-25T00:00:00Z", s)
	})

	t.Run("Datetime64RoundTrip", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT $p, CAST($p AS Text)`, sql.Named("p",
			time.Date(2024, 12, 25, 18, 30, 0, 0, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(2024, 12, 25, 18, 30, 0, 0, time.UTC).Unix(),
			dst.UTC().Unix(),
		)
		require.Equal(t, "2024-12-25T18:30:00Z", s)
	})

	t.Run("Timestamp64RoundTrip", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT $p, CAST($p AS Text)`, sql.Named("p",
			time.Date(2024, 12, 25, 18, 30, 45, 987654000, time.UTC),
		))
		require.NoError(t, row.Err())

		var (
			dst time.Time
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t,
			time.Date(2024, 12, 25, 18, 30, 45, 987654000, time.UTC).Unix(),
			dst.UTC().Unix(),
		)
		require.Equal(t, 987654000/1000, dst.UTC().Nanosecond()/1000)
		require.Equal(t, "2024-12-25T18:30:45.987654Z", s)
	})

	t.Run("Interval64RoundTrip", func(t *testing.T) {
		row := db.QueryRowContext(ctx, `SELECT $p, CAST($p AS Text)`, sql.Named("p",
			7*time.Hour+45*time.Minute+30*time.Second+999999*time.Microsecond,
		))
		require.NoError(t, row.Err())

		var (
			dst time.Duration
			s   string
		)
		require.NoError(t, row.Scan(&dst, &s))

		require.Equal(t, 7*time.Hour+45*time.Minute+30*time.Second+999999*time.Microsecond, dst)
		require.Equal(t, "PT7H45M30.999999S", s)
	})
}
