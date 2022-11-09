package table_test

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func Example_select() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	var (
		query = `SELECT 42 as id, "my string" as myStr`
		id    int32  // required value
		myStr string // optional value
	)
	err = db.Table().Do( // Do retry operation on errors with best effort
		ctx, // context manage exiting from Do
		func(ctx context.Context, s table.Session) (err error) { // retry operation
			_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, nil)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer res.Close()                                // cleanup resources
			if err = res.NextResultSetErr(ctx); err != nil { // check single result set and switch to it
				return err // for auto-retry with driver
			}
			for res.NextRow() { // iterate over rows
				err = res.ScanNamed(
					named.Required("id", &id),
					named.OptionalWithDefault("myStr", &myStr),
				)
				if err != nil {
					return err // generally scan error not retryable, return it for driver check error
				}
				fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
			}
			return res.Err() // return finally result error for auto-retry with driver
		},
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_createTable() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(db.Name(), "series"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeText)),
				options.WithColumn("series_info", types.Optional(types.TypeText)),
				options.WithColumn("release_date", types.Optional(types.TypeDate)),
				options.WithColumn("comment", types.Optional(types.TypeText)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_bulkUpsert() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	type logMessage struct {
		App       string
		Host      string
		Timestamp time.Time
		HTTPCode  uint32
		Message   string
	}
	// prepare native go data
	const batchSize = 10000
	logs := make([]logMessage, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		message := logMessage{
			App:       fmt.Sprintf("App_%d", i/256),
			Host:      fmt.Sprintf("192.168.0.%d", i%256),
			Timestamp: time.Now().Add(time.Millisecond * time.Duration(i%1000)),
			HTTPCode:  200,
		}
		if i%2 == 0 {
			message.Message = "GET / HTTP/1.1"
		} else {
			message.Message = "GET /images/logo.png HTTP/1.1"
		}
		logs = append(logs, message)
	}
	// execute bulk upsert with native ydb data
	err = db.Table().Do( // Do retry operation on errors with best effort
		ctx, // context manage exiting from Do
		func(ctx context.Context, s table.Session) (err error) { // retry operation
			rows := make([]types.Value, 0, len(logs))
			for _, msg := range logs {
				rows = append(rows, types.StructValue(
					types.StructFieldValue("App", types.TextValue(msg.App)),
					types.StructFieldValue("Host", types.TextValue(msg.Host)),
					types.StructFieldValue("Timestamp", types.TimestampValueFromTime(msg.Timestamp)),
					types.StructFieldValue("HTTPCode", types.Uint32Value(msg.HTTPCode)),
					types.StructFieldValue("Message", types.TextValue(msg.Message)),
				))
			}
			return s.BulkUpsert(ctx, "/local/bulk_upsert_example", types.ListValue(rows...))
		},
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_alterTable() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.AlterTable(ctx, path.Join(db.Name(), "series"),
				options.WithAddColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithAddColumn("title", types.Optional(types.TypeText)),
				options.WithAlterAttribute("hello", "world"),
				options.WithAddAttribute("foo", "bar"),
				options.WithDropAttribute("baz"),
			)
		},
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}
