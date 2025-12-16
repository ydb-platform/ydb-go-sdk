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
		// Date32 stores days since epoch as int32, supporting dates from year -4713 to 5874897
		testDate := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Date32)`, sql.Named("p", testDate))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		// Compare dates only (without time component)
		expectedDate := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
		require.Equal(t, expectedDate.Format("2006-01-02"), dst.UTC().Format("2006-01-02"))
	})

	t.Run("Date32Negative", func(t *testing.T) {
		// Test with a date before epoch (negative days)
		testDate := time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Date32)`, sql.Named("p", testDate))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		expectedDate := time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, expectedDate.Format("2006-01-02"), dst.UTC().Format("2006-01-02"))
	})

	t.Run("Datetime64", func(t *testing.T) {
		// Datetime64 stores seconds since epoch as int64
		testDatetime := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Datetime64)`, sql.Named("p", testDatetime))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		// Datetime64 has second precision
		expectedDatetime := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)
		require.Equal(t, expectedDatetime.Unix(), dst.UTC().Unix())
	})

	t.Run("Datetime64Negative", func(t *testing.T) {
		// Test with a datetime before epoch (negative seconds)
		testDatetime := time.Date(1960, 3, 20, 10, 15, 30, 0, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Datetime64)`, sql.Named("p", testDatetime))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		expectedDatetime := time.Date(1960, 3, 20, 10, 15, 30, 0, time.UTC)
		require.Equal(t, expectedDatetime.Unix(), dst.UTC().Unix())
	})

	t.Run("Timestamp64", func(t *testing.T) {
		// Timestamp64 stores microseconds since epoch as int64
		testTimestamp := time.Date(2024, 6, 15, 14, 30, 45, 123456000, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Timestamp64)`, sql.Named("p", testTimestamp))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		// Timestamp64 has microsecond precision
		expectedTimestamp := time.Date(2024, 6, 15, 14, 30, 45, 123456000, time.UTC)
		require.Equal(t, expectedTimestamp.Unix(), dst.UTC().Unix())
		// Check microsecond precision (nanoseconds truncated to microseconds)
		require.Equal(t, expectedTimestamp.Nanosecond()/1000, dst.UTC().Nanosecond()/1000)
	})

	t.Run("Timestamp64Negative", func(t *testing.T) {
		// Test with a timestamp before epoch (negative microseconds)
		testTimestamp := time.Date(1965, 7, 10, 8, 20, 15, 500000000, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Timestamp64)`, sql.Named("p", testTimestamp))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		expectedTimestamp := time.Date(1965, 7, 10, 8, 20, 15, 500000000, time.UTC)
		require.Equal(t, expectedTimestamp.Unix(), dst.UTC().Unix())
		require.Equal(t, expectedTimestamp.Nanosecond()/1000, dst.UTC().Nanosecond()/1000)
	})

	t.Run("Interval64", func(t *testing.T) {
		// Interval64 stores nanoseconds as int64
		testInterval := 5*time.Hour + 30*time.Minute + 45*time.Second + 123456789*time.Nanosecond

		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Interval64)`, sql.Named("p", testInterval))
		require.NoError(t, row.Err())

		var dst time.Duration
		require.NoError(t, row.Scan(&dst))

		// Interval64 has nanosecond precision
		require.Equal(t, testInterval, dst)
	})

	t.Run("Interval64Negative", func(t *testing.T) {
		// Test with a negative interval
		testInterval := -(2*time.Hour + 15*time.Minute + 30*time.Second)

		row := db.QueryRowContext(ctx, `SELECT CAST($p AS Interval64)`, sql.Named("p", testInterval))
		require.NoError(t, row.Err())

		var dst time.Duration
		require.NoError(t, row.Scan(&dst))

		require.Equal(t, testInterval, dst)
	})

	t.Run("Date32RoundTrip", func(t *testing.T) {
		// Test sending Date32 parameter and reading it back
		testDate := time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT $p`, sql.Named("p", testDate))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		// Date comparison (ignoring time component)
		require.Equal(t, testDate.Format("2006-01-02"), dst.UTC().Format("2006-01-02"))
	})

	t.Run("Datetime64RoundTrip", func(t *testing.T) {
		// Test sending Datetime64 parameter and reading it back
		testDatetime := time.Date(2024, 12, 25, 18, 30, 0, 0, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT $p`, sql.Named("p", testDatetime))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		// Second precision comparison
		require.Equal(t, testDatetime.Unix(), dst.UTC().Unix())
	})

	t.Run("Timestamp64RoundTrip", func(t *testing.T) {
		// Test sending Timestamp64 parameter and reading it back
		testTimestamp := time.Date(2024, 12, 25, 18, 30, 45, 987654000, time.UTC)

		row := db.QueryRowContext(ctx, `SELECT $p`, sql.Named("p", testTimestamp))
		require.NoError(t, row.Err())

		var dst time.Time
		require.NoError(t, row.Scan(&dst))

		// Microsecond precision comparison
		require.Equal(t, testTimestamp.Unix(), dst.UTC().Unix())
		require.Equal(t, testTimestamp.Nanosecond()/1000, dst.UTC().Nanosecond()/1000)
	})

	t.Run("Interval64RoundTrip", func(t *testing.T) {
		// Test sending Interval64 parameter and reading it back
		testInterval := 7*time.Hour + 45*time.Minute + 30*time.Second + 999999999*time.Nanosecond

		row := db.QueryRowContext(ctx, `SELECT $p`, sql.Named("p", testInterval))
		require.NoError(t, row.Err())

		var dst time.Duration
		require.NoError(t, row.Scan(&dst))

		require.Equal(t, testInterval, dst)
	})
}
