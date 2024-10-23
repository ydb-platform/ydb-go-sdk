//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type rawInt64 struct {
	val int64
}

func (r *rawInt64) UnmarshalYDB(raw types.RawValue) error {
	raw.Unwrap()
	r.val = raw.Int64()
	return nil
}

func TestIssueWideResultWithUnwrap(t *testing.T) {
	const columnsCount = 200
	var columns []string
	for i := 0; i < columnsCount; i++ {
		column := fmt.Sprintf("CAST(%v AS Int64?) as c%v", i, i)
		columns = append(columns, column)
	}

	query := "SELECT " + strings.Join(columns, ", ")
	t.Run("named", func(t *testing.T) {
		scope := newScope(t)
		var res result.Result
		var err error
		err = scope.Driver().Table().DoTx(scope.Ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err = tx.Execute(ctx, query, nil)
			return err
		})
		require.NoError(t, err)

		res.NextResultSet(scope.Ctx)
		require.NoError(t, res.Err())
		res.NextRow()
		require.NoError(t, res.Err())

		results := make([]rawInt64, columnsCount)
		resultsPointers := make([]named.Value, columnsCount)
		for i := range results {
			resultsPointers[i] = named.Required("c"+strconv.Itoa(i), &results[i])
		}
		err = res.ScanNamed(resultsPointers...)
		require.NoError(t, err)

		for i, val := range results {
			require.Equal(t, int64(i), val.val)
		}
	})
	t.Run("indexed", func(t *testing.T) {
		scope := newScope(t)
		var res result.Result
		var err error
		err = scope.Driver().Table().DoTx(scope.Ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err = tx.Execute(ctx, query, nil)
			return err
		})
		require.NoError(t, err)

		res.NextResultSet(scope.Ctx)
		require.NoError(t, res.Err())
		res.NextRow()
		require.NoError(t, res.Err())

		results := make([]rawInt64, columnsCount)
		resultsPointers := make([]indexed.RequiredOrOptional, columnsCount)
		for i := range results {
			resultsPointers[i] = &results[i]
		}
		err = res.Scan(resultsPointers...)
		require.NoError(t, err)

		for i, val := range results {
			require.Equal(t, int64(i), val.val)
		}
	})
}

// https://github.com/ydb-platform/ydb-go-sdk/issues/1227
func TestRegressionIssue1227RetryBadSession(t *testing.T) {
	var (
		scope = newScope(t)
		cnt   = 0
	)
	err := scope.Driver().Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		cnt++
		if cnt < 100 {
			return xerrors.WithStackTrace(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)))
		}

		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, 100, cnt)
}

func TestUUIDSerializationTableServiceServiceIssue1501(t *testing.T) {
	// https://github.com/ydb-platform/ydb-go-sdk/issues/1501
	// test with special uuid - all bytes are different for check any byte swaps

	t.Run("old-send", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := "2d9e498b-b746-9cfb-084d-de4e1cb4736e"
		id := uuid.MustParse(idString)

		var idFromDB string
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS UUID;

SELECT CAST($val AS Utf8)
`, table.NewQueryParameters(table.ValueParam("$val", types.UUIDValue(id))))
			if err != nil {
				return err
			}
			res.NextResultSet(ctx)
			res.NextRow()

			err = res.Scan(&idFromDB)
			return err
		})
		require.NoError(t, err)
		require.Equal(t, expectedResultWithBug, idFromDB)
	})
	t.Run("old-send-with-force-wrapper", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := "2d9e498b-b746-9cfb-084d-de4e1cb4736e"
		id := uuid.MustParse(idString)

		var idFromDB string
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS UUID;

SELECT CAST($val AS Utf8)
`, table.NewQueryParameters(table.ValueParam("$val", types.UUIDWithIssue1501Value(id))))
			if err != nil {
				return err
			}
			res.NextResultSet(ctx)
			res.NextRow()

			err = res.Scan(&idFromDB)
			return err
		})
		require.NoError(t, err)
		require.Equal(t, expectedResultWithBug, idFromDB)
	})
	t.Run("old-receive-to-bytes", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := "8b499e2d-46b7-fb9c-4d08-4ede6e73b41c"
		var resultFromDb uuid.UUID
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS Text;

SELECT CAST($val AS UUID)
`, table.NewQueryParameters(table.ValueParam("$val", types.TextValue(idString))))
			if err != nil {
				return err
			}

			res.NextResultSet(ctx)
			res.NextRow()

			var resBytes [16]byte
			err = res.Scan(&resBytes)
			if err != nil {
				return err
			}

			resultFromDb = resBytes
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, expectedResultWithBug, resultFromDb.String())
	})
	t.Run("old-receive-to-bytes-with-force-wrapper", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := "8b499e2d-46b7-fb9c-4d08-4ede6e73b41c"
		var resultFromDb uuid.UUID
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS Text;

SELECT CAST($val AS UUID)
`, table.NewQueryParameters(table.ValueParam("$val", types.TextValue(idString))))
			if err != nil {
				return err
			}

			res.NextResultSet(ctx)
			res.NextRow()

			var resBytes [16]byte
			var bytesWrapper *types.UUIDBytesWithIssue1501Type
			err = res.Scan(&bytesWrapper)
			if err != nil {
				return err
			}
			resBytes = bytesWrapper.AsBytesArray()

			resultFromDb = resBytes
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, expectedResultWithBug, resultFromDb.String())
	})
	t.Run("old-unmarshal-with-bytes", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := "8b499e2d-46b7-fb9c-4d08-4ede6e73b41c"
		var resultFromDb customTestUnmarshalUUIDFromFixedBytes
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS Text;

SELECT CAST($val AS UUID)
`, table.NewQueryParameters(table.ValueParam("$val", types.TextValue(idString))))
			if err != nil {
				return err
			}

			res.NextResultSet(ctx)
			res.NextRow()

			err = res.Scan(&resultFromDb)
			if err != nil {
				return err
			}

			return nil
		})

		require.NoError(t, err)
		require.Equal(t, expectedResultWithBug, resultFromDb.String())
	})
	t.Run("old-unmarshal-with-bytes-force-wrapper", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := "8b499e2d-46b7-fb9c-4d08-4ede6e73b41c"
		var resultFromDb customTestUnmarshalUUIDWithForceWrapper
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS Text;

SELECT CAST($val AS UUID)
`, table.NewQueryParameters(table.ValueParam("$val", types.TextValue(idString))))
			if err != nil {
				return err
			}

			res.NextResultSet(ctx)
			res.NextRow()

			err = res.Scan(&resultFromDb)
			if err != nil {
				return err
			}

			return nil
		})

		require.NoError(t, err)
		require.Equal(t, expectedResultWithBug, resultFromDb.String())
	})
	t.Run("old-unmarshal-with-bytes-typed", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		var resultFromDb customTestUnmarshalUUIDTyped
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS Text;

SELECT CAST($val AS UUID)
`, table.NewQueryParameters(table.ValueParam("$val", types.TextValue(idString))))
			if err != nil {
				return err
			}

			res.NextResultSet(ctx)
			res.NextRow()

			err = res.Scan(&resultFromDb)
			if err != nil {
				return err
			}

			return nil
		})

		require.NoError(t, err)
		require.Equal(t, strings.ToUpper(idString), strings.ToUpper(resultFromDb.String()))
	})

	t.Run("old-receive-to-string", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := []byte{0x8b, 0x49, 0x9e, 0x2d, 0x46, 0xb7, 0xfb, 0x9c, 0x4d, 0x8, 0x4e, 0xde, 0x6e, 0x73, 0xb4, 0x1c}
		var resultFromDb string
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS Text;

SELECT CAST($val AS UUID)
`, table.NewQueryParameters(table.ValueParam("$val", types.TextValue(idString))))
			if err != nil {
				return err
			}

			res.NextResultSet(ctx)
			res.NextRow()

			err = res.ScanWithDefaults(&resultFromDb)
			if err != nil {
				return err
			}

			return nil
		})

		require.NoError(t, err)
		resultBytes := []byte(resultFromDb)
		require.Equal(t, expectedResultWithBug, resultBytes)
	})
	t.Run("old-receive-to-string-with-force-wrapper", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := []byte{0x8b, 0x49, 0x9e, 0x2d, 0x46, 0xb7, 0xfb, 0x9c, 0x4d, 0x8, 0x4e, 0xde, 0x6e, 0x73, 0xb4, 0x1c}
		var resultFromDb string
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS Text;

SELECT CAST($val AS UUID)
`, table.NewQueryParameters(table.ValueParam("$val", types.TextValue(idString))))
			if err != nil {
				return err
			}

			res.NextResultSet(ctx)
			res.NextRow()

			var wrapper types.UUIDBytesWithIssue1501Type
			err = res.ScanWithDefaults(&wrapper)
			if err != nil {
				return err
			}

			resultFromDb = wrapper.AsBrokenString()

			return nil
		})

		require.NoError(t, err)
		resultBytes := []byte(resultFromDb)
		require.Equal(t, expectedResultWithBug, resultBytes)
	})
	t.Run("old-send-receive", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := uuid.MustParse(idString)

		var idFromDB uuid.UUID
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS UUID;

SELECT $val
`, table.NewQueryParameters(table.ValueParam("$val", types.UUIDValue(id))))
			if err != nil {
				return err
			}
			res.NextResultSet(ctx)
			res.NextRow()

			var resBytes [16]byte
			err = res.Scan(&resBytes)
			if err != nil {
				return err
			}
			idFromDB = resBytes
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, id, idFromDB)
	})
	t.Run("old-send-receive-with-force-wrapper", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := uuid.MustParse(idString)

		var idFromDB uuid.UUID
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS UUID;

SELECT $val
`, table.NewQueryParameters(table.ValueParam("$val", types.UUIDWithIssue1501Value(id))))
			if err != nil {
				return err
			}
			res.NextResultSet(ctx)
			res.NextRow()

			var resBytes [16]byte
			var resWrapper types.UUIDBytesWithIssue1501Type
			err = res.Scan(&resWrapper)
			if err != nil {
				return err
			}
			resBytes = resWrapper.AsBytesArray()

			idFromDB = resBytes
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, id, idFromDB)
	})
	t.Run("good-send", func(t *testing.T) {
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := uuid.MustParse(idString)
		var resStringId string
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS UUID;

SELECT CAST($val AS Utf8)
`, table.NewQueryParameters(table.ValueParam("$val", types.UuidValue(id))))

			res.NextResultSet(ctx)
			res.NextRow()
			err = res.ScanWithDefaults(&resStringId)
			if err != nil {
				return err
			}
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, strings.ToUpper(id.String()), strings.ToUpper(resStringId))
	})
	t.Run("good-receive", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"

		var resID uuid.UUID
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS Text;

SELECT CAST($val AS UUID)
`, table.NewQueryParameters(table.ValueParam("$val", types.TextValue(idString))))
			if err != nil {
				return err
			}
			res.NextResultSet(ctx)
			res.NextRow()
			return res.ScanWithDefaults(&resID)
		})

		require.NoError(t, err)
		require.Equal(t, strings.ToUpper(idString), strings.ToUpper(resID.String()))
	})
	t.Run("good-send-receive", func(t *testing.T) {
		var (
			scope = newScope(t)
			ctx   = scope.Ctx
			db    = scope.Driver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := uuid.MustParse(idString)
		var resID uuid.UUID
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, `
DECLARE $val AS UUID;

SELECT $val
`, table.NewQueryParameters(table.ValueParam("$val", types.UuidValue(id))))

			res.NextResultSet(ctx)
			res.NextRow()
			err = res.ScanWithDefaults(&resID)
			if err != nil {
				return err
			}
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, err)
		require.Equal(t, strings.ToUpper(idString), strings.ToUpper(resID.String()))
	})
}

type customTestUnmarshalUUIDFromFixedBytes string

func (c *customTestUnmarshalUUIDFromFixedBytes) UnmarshalYDB(raw scanner.RawValue) error {
	val := raw.UUID()
	id := uuid.UUID(val)
	*c = customTestUnmarshalUUIDFromFixedBytes(id.String())
	return raw.Err()
}

func (c *customTestUnmarshalUUIDFromFixedBytes) String() string {
	return string(*c)
}

type customTestUnmarshalUUIDWithForceWrapper string

func (c *customTestUnmarshalUUIDWithForceWrapper) UnmarshalYDB(raw scanner.RawValue) error {
	val := raw.UUIDWithIssue1501()
	id := uuid.UUID(val)
	*c = customTestUnmarshalUUIDWithForceWrapper(id.String())
	return raw.Err()
}

func (c *customTestUnmarshalUUIDWithForceWrapper) String() string {
	return string(*c)
}

type customTestUnmarshalUUIDTyped string

func (c *customTestUnmarshalUUIDTyped) UnmarshalYDB(raw scanner.RawValue) error {
	val := raw.UUIDTyped()
	*c = customTestUnmarshalUUIDTyped(val.String())
	return raw.Err()
}

func (c *customTestUnmarshalUUIDTyped) String() string {
	return string(*c)
}
