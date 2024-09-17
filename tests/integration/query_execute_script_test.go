//go:build integration
// +build integration

package integration

import (
	"errors"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestQueryExecuteScript(sourceTest *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		sourceTest.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	t := xtest.MakeSyncedTest(sourceTest)
	var (
		folder           = t.Name()
		tableName        = `test`
		db               *ydb.Driver
		err              error
		upsertRowsCount  = 100000
		batchSize        = 10000
		expectedCheckSum = uint64(4999950000)
		ctx              = xtest.Context(t)
	)

	db, err = ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func(db *ydb.Driver) {
		// cleanup
		_ = db.Close(ctx)
	}(db)

	err = db.Query().Exec(ctx,
		"CREATE TABLE IF NOT EXISTS `"+path.Join(db.Name(), folder, tableName)+"` (val Int64, PRIMARY KEY (val))",
	)
	require.NoError(t, err)

	require.Zero(t, upsertRowsCount%batchSize, "wrong batch size: (%d mod %d = %d) != 0", upsertRowsCount, batchSize, upsertRowsCount%batchSize)

	var upserted uint32
	for i := 0; i < (upsertRowsCount / batchSize); i++ {
		var (
			from = int32(i * batchSize)
			to   = int32((i + 1) * batchSize)
		)
		t.Logf("upserting rows %d..%d\n", from, to-1)
		values := make([]types.Value, 0, batchSize)
		for j := from; j < to; j++ {
			values = append(
				values,
				types.StructValue(
					types.StructFieldValue("val", types.Int32Value(j)),
				),
			)
		}
		err := db.Query().Exec(ctx, `
						DECLARE $values AS List<Struct<
							val: Int32,
						>>;
						UPSERT INTO `+"`"+path.Join(db.Name(), folder, tableName)+"`"+`
						SELECT
							val 
						FROM
							AS_TABLE($values);            
					`, query.WithParameters(
			ydb.ParamsBuilder().Param("$values").BeginList().AddItems(values...).EndList().Build(),
		),
		)
		require.NoError(t, err)
		upserted += uint32(to - from)
	}
	require.Equal(t, uint32(upsertRowsCount), upserted)

	row, err := db.Query().QueryRow(ctx,
		"SELECT CAST(COUNT(*) AS Uint64) FROM `"+path.Join(db.Name(), folder, tableName)+"`;",
	)
	require.NoError(t, err)
	var rowsFromDb uint64
	err = row.Scan(&rowsFromDb)
	require.NoError(t, err)
	require.Equal(t, uint64(upsertRowsCount), rowsFromDb)

	row, err = db.Query().QueryRow(ctx,
		"SELECT CAST(SUM(val) AS Uint64) FROM `"+path.Join(db.Name(), folder, tableName)+"`;",
	)
	require.NoError(t, err)
	var checkSumFromDb uint64
	err = row.Scan(&checkSumFromDb)
	require.NoError(t, err)
	require.Equal(t, expectedCheckSum, checkSumFromDb)

	op, err := db.Query().ExecuteScript(ctx,
		"SELECT val FROM `"+path.Join(db.Name(), folder, tableName)+"`;",
		time.Hour,
	)
	require.NoError(t, err)

	for {
		status, err := db.Operation().Get(ctx, op.ID)
		require.NoError(t, err)
		if status.Ready {
			break
		}
		time.Sleep(time.Second)
	}

	var (
		nextToken string
		rowsCount = 0
		checkSum  = uint64(0)
	)
	for {
		result, err := db.Query().FetchScriptResults(ctx, op.ID,
			query.WithResultSetIndex(0),
			query.WithRowsLimit(1000),
			query.WithFetchToken(nextToken),
		)
		require.NoError(t, err)
		nextToken = result.NextToken
		require.EqualValues(t, 0, result.ResultSetIndex)
		t.Logf("reading next 1000 rows. Current rows count: %v\n", rowsCount)
		for {
			row, err := result.ResultSet.NextRow(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatal(err)
			}
			rowsCount++
			var val int64
			err = row.Scan(&val)
			checkSum += uint64(val)
		}
		if result.NextToken == "" {
			break
		}
	}
	require.EqualValues(t, upsertRowsCount, rowsCount)
	require.EqualValues(t, expectedCheckSum, checkSum)
}
