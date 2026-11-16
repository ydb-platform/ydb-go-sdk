package test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// twoStatementsSQL exercises the multi-statement contract end-to-end with
// heterogeneous result sets:
//   - first  RS: 1 column, INT32              → "id"    = 42
//   - second RS: 2 columns, UTF8 + STRING     → "hello" = "hello", "world" = "world"
const twoStatementsSQL = `SELECT 42 AS id; SELECT "hello"u AS hello, "world" AS world`

// TestTwoStatementsScanStruct verifies that ScanStruct works
// correctly for each result set returned by a query containing two SELECT
// statements when accessed via the native query service API. Each result set
// has its own column layout, so the test binds different struct types per
// result set after switching via result.NextResultSet.
func TestTwoStatementsScanStruct(t *testing.T) {
	mockSrv := mock.Server(t)

	openCtx := t.Context()

	nativeDriver, err := ydb.Open(openCtx, mockSrv.ConnString(),
		ydb.WithAnonymousCredentials(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = nativeDriver.Close(openCtx)
	})

	type firstResultSet struct {
		ID int32 `sql:"id"`
	}

	type secondResultSet struct {
		Hello string `sql:"hello"`
		World []byte `sql:"world"`
	}

	var (
		first  firstResultSet
		second secondResultSet
	)

	err = nativeDriver.Query().Do(openCtx, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, twoStatementsSQL)
		if err != nil {
			return err
		}
		defer func() {
			_ = result.Close(ctx)
		}()

		rs, err := result.NextResultSet(ctx)
		if err != nil {
			return err
		}
		row, err := rs.NextRow(ctx)
		if err != nil {
			return err
		}
		if err := row.ScanStruct(&first); err != nil {
			return err
		}

		rs, err = result.NextResultSet(ctx)
		if err != nil {
			return err
		}
		row, err = rs.NextRow(ctx)
		if err != nil {
			return err
		}
		if err := row.ScanStruct(&second); err != nil {
			return err
		}

		_, err = result.NextResultSet(ctx)
		if err != nil && errors.Is(err, io.EOF) {
			return nil
		}

		return err
	})
	require.NoError(t, err)
	require.Equal(t, int32(42), first.ID)
	require.Equal(t, "hello", second.Hello)
	require.Equal(t, []byte("world"), second.World)
}

// TestTwoStatementsScanNamed mirrors the ScanStruct scenario but
// uses ScanNamed to extract column values from each of the two result sets.
// The destinations have different Go types per result set (int32 vs string +
// []byte), reflecting the heterogeneous column layout.
func TestTwoStatementsScanNamed(t *testing.T) {
	mockSrv := mock.Server(t)

	openCtx := t.Context()

	nativeDriver, err := ydb.Open(openCtx, mockSrv.ConnString(),
		ydb.WithAnonymousCredentials(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = nativeDriver.Close(openCtx)
	})

	var (
		id    int32
		hello string
		world []byte
	)

	err = nativeDriver.Query().Do(openCtx, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, twoStatementsSQL)
		if err != nil {
			return err
		}
		defer func() {
			_ = result.Close(ctx)
		}()

		rs, err := result.NextResultSet(ctx)
		if err != nil {
			return err
		}
		row, err := rs.NextRow(ctx)
		if err != nil {
			return err
		}
		if err := row.ScanNamed(query.Named("id", &id)); err != nil {
			return err
		}

		rs, err = result.NextResultSet(ctx)
		if err != nil {
			return err
		}
		row, err = rs.NextRow(ctx)
		if err != nil {
			return err
		}
		if err := row.ScanNamed(
			query.Named("hello", &hello),
			query.Named("world", &world),
		); err != nil {
			return err
		}
		_, err = result.NextResultSet(ctx)
		if err != nil && errors.Is(err, io.EOF) {
			return nil
		}

		return err
	})
	require.NoError(t, err)
	require.Equal(t, int32(42), id)
	require.Equal(t, "hello", hello)
	require.Equal(t, []byte("world"), world)
}
