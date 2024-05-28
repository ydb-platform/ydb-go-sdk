package table_test

import (
	"context"
	"fmt"
	"path"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func Example_select() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
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
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_explainQuery() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		plan string
		ast  string
	)
	err = db.Table().Do( // Do retry operation on errors with best effort
		ctx, // context manage exiting from Do
		func(ctx context.Context, s table.Session) (err error) { // retry operation
			explanation, err := s.Explain(ctx, `
				DECLARE $id AS Optional<Uint64>;
				DECLARE $myStr AS Optional<Text>;
				SELECT $id AS id, $myStr AS myStr;
			`)
			if err != nil {
				return err // for auto-retry with driver
			}

			plan, ast = explanation.Plan, explanation.AST

			return nil
		},
		table.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Plan: %s\n", plan)
	fmt.Printf("AST: %s\n", ast)
}

func Example_createTable() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
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
				options.WithColumn("expire_at", types.Optional(types.TypeDate)),
				options.WithColumn("comment", types.Optional(types.TypeText)),
				options.WithPrimaryKeyColumn("series_id"),
				options.WithTimeToLiveSettings(
					options.NewTTLSettings().ColumnDateType("expire_at").ExpireAfter(time.Hour),
				),
				options.WithIndex("idx_series_title",
					options.WithIndexColumns("title"),
					options.WithIndexType(options.GlobalAsyncIndex()),
				),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_bulkUpsert() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
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
		logs = append(logs, logMessage{
			App:       fmt.Sprintf("App_%d", i/256),
			Host:      fmt.Sprintf("192.168.0.%d", i%256),
			Timestamp: time.Now().Add(time.Millisecond * time.Duration(i%1000)),
			HTTPCode:  200,
			Message:   "GET / HTTP/1.1",
		})
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
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_alterTable() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
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
				options.WithSetTimeToLiveSettings(
					options.NewTTLSettings().ColumnDateType("expire_at").ExpireAfter(time.Hour),
				),
				options.WithDropTimeToLive(),
				options.WithAddIndex("idx_series_series_id",
					options.WithIndexColumns("series_id"),
					options.WithDataColumns("title"),
					options.WithIndexType(options.GlobalIndex()),
				),
				options.WithDropIndex("idx_series_title"),
				options.WithAlterAttribute("hello", "world"),
				options.WithAddAttribute("foo", "bar"),
				options.WithDropAttribute("baz"),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_lazyTransaction() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)

		return
	}
	defer db.Close(ctx)
	err = db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			// execute query with opening lazy transaction
			tx, result, err := session.Execute(ctx,
				table.SerializableReadWriteTxControl(),
				"DECLARE $id AS Uint64; "+
					"SELECT `title`, `description` FROM `path/to/mytable` WHERE id = $id",
				table.NewQueryParameters(
					table.ValueParam("$id", types.Uint64Value(1)),
				),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = tx.Rollback(ctx)
				_ = result.Close()
			}()
			if !result.NextResultSet(ctx) {
				return retry.RetryableError(fmt.Errorf("no result sets"))
			}
			if !result.NextRow() {
				return retry.RetryableError(fmt.Errorf("no rows"))
			}
			var (
				id          uint64
				title       string
				description string
			)
			if err = result.ScanNamed(
				named.OptionalWithDefault("id", &id),
				named.OptionalWithDefault("title", &title),
				named.OptionalWithDefault("description", &description),
			); err != nil {
				return err
			}
			fmt.Println(id, title, description)
			// execute query with commit transaction
			_, err = tx.Execute(ctx,
				"DECLARE $id AS Uint64; "+
					"DECLARE $description AS Text; "+
					"UPSERT INTO `path/to/mytable` "+
					"(id, description) "+
					"VALUES ($id, $description);",
				table.NewQueryParameters(
					table.ValueParam("$id", types.Uint64Value(1)),
					table.ValueParam("$description", types.TextValue("changed description")),
				),
				options.WithCommit(),
			)
			if err != nil {
				return err
			}

			return result.Err()
		},
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_bulkUpsertWithCompression() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
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
		logs = append(logs, logMessage{
			App:       fmt.Sprintf("App_%d", i/256),
			Host:      fmt.Sprintf("192.168.0.%d", i%256),
			Timestamp: time.Now().Add(time.Millisecond * time.Duration(i%1000)),
			HTTPCode:  200,
			Message:   "GET /images/logo.png HTTP/1.1",
		})
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

			return s.BulkUpsert(ctx, "/local/bulk_upsert_example", types.ListValue(rows...),
				options.WithCallOptions(grpc.UseCompressor(gzip.Name)),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_dataQueryWithCompression() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
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
			_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, nil,
				options.WithCallOptions(
					grpc.UseCompressor(gzip.Name),
				),
			)
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
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_scanQueryWithCompression() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
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
			res, err := s.StreamExecuteScanQuery(ctx, query, nil,
				options.WithCallOptions(
					grpc.UseCompressor(gzip.Name),
				),
			)
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
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}

func Example_copyTables() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CopyTables(ctx,
				options.CopyTablesItem(
					path.Join(db.Name(), "from", "series"),
					path.Join(db.Name(), "to", "series"),
					true,
				),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}
