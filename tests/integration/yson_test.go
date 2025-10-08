//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

//  CREATE TABLE `test_yson_records` (
//     id SERIAL NOT NULL,
//     test_string Utf8 NOT NULL,
//     test_date Yson NOT NULL,
//     created_at Timestamp NOt NUll,
// PRIMARY KEY ( id)
// )
//

func TestYSONWrite(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver()
	ctx := scope.Ctx

	var recordID int32

	err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		query := `
			INSERT INTO test_yson_records (
				test_string,
				test_date,
				created_at
			) VALUES (
				$test_string,
				Yson::FromStruct($test_date),
				CurrentUtcTimestamp()
			) RETURNING id;`

		var dateStruct types.Value

		dateStruct = types.StructValue(
			types.StructFieldValue("Year", types.Int32Value(2020)),
			types.StructFieldValue("Month", types.Int32Value(10)),
			types.StructFieldValue("Day", types.Int32Value(31)),
		)

		params := table.NewQueryParameters(
			table.ValueParam("$test_string", types.UTF8Value("abc")),
			table.ValueParam("$test_date", dateStruct),
		)

		_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, params, options.WithCollectStatsModeBasic())
		if err != nil {
			return err
		}

		if err := res.NextResultSetErr(ctx); err != nil {
			return fmt.Errorf("failed to get result set: %w", err)
		}

		if !res.NextRow() {
			return fmt.Errorf("no rows returned from insert")
		}

		if err := res.ScanWithDefaults(&recordID); err != nil {
			return fmt.Errorf("failed to scan id: %w", err)
		}

		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 100500, recordID)
}

type TDate struct {
	Year  int32
	Month int32
	Day   int32
}

type TestYsonData struct {
	TestString string
	TestDate   *TDate
}

func TestYSONRead_TableClient(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver()
	ctx := scope.Ctx

	err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		query := `
			SELECT
				test_string,
				test_date
			FROM test_yson_records
			WHERE id = $record_id;`

		params := table.NewQueryParameters(
			table.ValueParam("$record_id", types.Int32Value(1)),
		)

		_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, params, options.WithCollectStatsModeBasic())
		require.NoError(t, err)

		if err := res.NextResultSetErr(ctx); err != nil {
			return fmt.Errorf("failed to get result set: %w", err)
		}

		if !res.NextRow() {
			return fmt.Errorf("record with ID %d not found", 2)
		}

		// Сканируем простое поле
		var testString string
		var testDateYson []byte

		err = res.ScanWithDefaults(&testString, &testDateYson)
		if err != nil {
			return fmt.Errorf("failed to scan fields: %w", err)
		}

		assert.NotEmpty(t, testDateYson)

		return nil
	})

	require.NoError(t, err)
}

func TestYSONRead_queryService(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver()
	ctx := scope.Ctx

	err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		q := `
			SELECT
				test_date
			FROM test_yson_records
			WHERE id = $record_id;`

		params := query.WithParameters(
			ydb.ParamsBuilder().
				Param("$record_id").Int32(1).
				Build(),
		)

		res, err := s.Query(ctx, q, params)
		require.NoError(t, err)

		rs, err := res.NextResultSet(ctx)
		require.NoError(t, err)

		row, err := rs.NextRow(ctx)
		require.NoError(t, err)

		var testDateYson []byte

		err = row.Scan(&testDateYson)
		require.NoError(t, err)

		assert.NotEmpty(t, testDateYson)

		return nil
	})

	require.NoError(t, err)
}
