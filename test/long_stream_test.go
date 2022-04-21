//go:build !fast
// +build !fast

package test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// nolint:gocyclo
func TestLongStream(t *testing.T) {
	discoveryInterval := 10 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 10*discoveryInterval)
	defer cancel()

	db, err := ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
		ydb.WithDiscoveryInterval(0), // disable rediscovery on upsert time
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func(db ydb.Connection) {
		// cleanup
		_ = db.Close(ctx)
	}(db)

	// - create table
	t.Logf("> creating table stream_query...\n")
	if err = db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.ExecuteSchemeQuery(
				ctx,
				`CREATE TABLE stream_query (val Int32, PRIMARY KEY (val))`,
			)
		},
	); err != nil {
		t.Fatalf("create table failed: %v\n", err)
	}
	t.Logf("> table stream_query created\n")
	var (
		upsertRowsCount = 50000
		batchSize       = 1000
	)

	if upsertRowsCount%batchSize != 0 {
		t.Fatalf("wrong batch size: (%d mod %d = %d) != 0", upsertRowsCount, batchSize, upsertRowsCount%batchSize)
	}

	// - upsert data
	wg := sync.WaitGroup{}
	semaphore := make(chan struct{}, 10)
	defer close(semaphore)
	for i := 0; i < (upsertRowsCount / batchSize); i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() {
				<-semaphore
			}()
			values := make([]types.Value, 0, upsertRowsCount)
			for j := 0; j < upsertRowsCount; j++ {
				values = append(
					values,
					types.StructValue(
						types.StructFieldValue("val", types.Int32Value(int32(i*batchSize+j))),
					),
				)
			}
			t.Logf("> upserting batch %d\n", i)
			if err = db.Table().Do(
				ctx,
				func(ctx context.Context, s table.Session) (err error) {
					_, _, err = s.Execute(
						ctx,
						table.TxControl(
							table.BeginTx(
								table.WithSerializableReadWrite(),
							),
							table.CommitTx(),
						),
						`
						DECLARE $values AS List<Struct<
							val: Int32,
						> >;
						UPSERT INTO stream_query
						SELECT
							val 
						FROM
							AS_TABLE($values);            
						`,
						table.NewQueryParameters(
							table.ValueParam(
								"$values",
								types.ListValue(values...),
							),
						),
					)
					return err
				},
			); err != nil {
				t.Fatalf("upsert failed: %v\n", err)
			}
			t.Logf("> batch %d upserted\n", i)
		}(i)
	}
	wg.Wait()

	// - make new connection with re-discoveries
	t.Logf("> child connection opening...\n")
	db, err = db.With(ctx, ydb.WithDiscoveryInterval(discoveryInterval))
	if err != nil {
		t.Fatal(err)
	}
	defer func(db ydb.Connection) {
		// cleanup
		_ = db.Close(ctx)
	}(db)
	t.Logf("> child connection opened\n")

	// - stream query
	t.Logf("> execute stream request...\n")
	if err = db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			start := time.Now()
			res, err := s.StreamExecuteScanQuery(ctx, "SELECT val FROM stream_query", table.NewQueryParameters())
			if err != nil {
				return err
			}
			var (
				setsCount = 0
				rowsCount = 0
			)
			for res.NextResultSet(ctx) {
				setsCount++
				t.Logf("> received result set %d\n", setsCount)
				for res.NextRow() {
					rowsCount++
				}
				time.Sleep(discoveryInterval)
			}
			if err = res.Err(); err != nil {
				return fmt.Errorf("received error: %w (duration: %v)", err, time.Since(start))
			}
			if rowsCount != upsertRowsCount {
				return fmt.Errorf("wrong rows count: %v (duration: %v)", rowsCount, time.Since(start))
			}
			return nil
		},
	); err != nil {
		t.Fatalf("stream query failed: %v\n", err)
	}
	t.Logf("> stream request executed\n")
}
