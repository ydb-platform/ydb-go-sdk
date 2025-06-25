//go:build go1.23

package ydb_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

//nolint:testableexamples, nonamedreturns
func Example_query() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources

	materializedResult, err := db.Query().Query( // Do retry operation on errors with best effort
		ctx, // context manage exiting from Do
		`SELECT $id as myId, $str as myStr`,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$id").Uint64(42).
				Param("$str").Text("my string").
				Build(),
		),
		query.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = materializedResult.Close(ctx) }() // cleanup resources
	for rs, err := range materializedResult.ResultSets(ctx) {
		if err != nil {
			panic(err)
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				panic(err)
			}
			type myStruct struct {
				ID  uint64 `sql:"id"`
				Str string `sql:"myStr"`
			}
			var s myStruct
			if err = row.ScanStruct(&s); err != nil {
				panic(err)
			}
		}
	}
}

//nolint:testableexamples, nonamedreturns
func Example_table() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		log.Fatal(err)
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
				log.Printf("id=%v, myStr='%s'\n", id, myStr)
			}

			return res.Err() // return finally result error for auto-retry with driver
		},
		table.WithIdempotent(),
	)
	if err != nil {
		log.Printf("unexpected error: %v", err)
	}
}

//nolint:testableexamples
func Example_databaseSQL() {
	db, err := sql.Open("ydb", "grpc://localhost:2136/local")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }() // cleanup resources

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	db.SetConnMaxIdleTime(time.Second) // workaround for background keep-aliving of YDB sessions

	var (
		id    int32  // required value
		myStr string // optional value
	)
	// retry transaction
	err = retry.DoTx(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `SELECT 42 as id, "my string" as myStr`)
		if err = row.Scan(&id, &myStr); err != nil {
			return err
		}
		log.Printf("id=%v, myStr='%s'\n", id, myStr)

		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		log.Printf("query failed: %v", err)
	}
}

//nolint:testableexamples
func Example_databaseSQLBindNumericArgs() {
	db, err := sql.Open("ydb",
		"grpc://localhost:2136/local?go_query_bind=declare,numeric",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }() // cleanup resources

	var (
		id    int32  // required value
		myStr string // optional value
	)

	// numeric args
	row := db.QueryRowContext(context.TODO(), "SELECT $2, $1", 42, "my string")
	if err = row.Scan(&myStr, &id); err != nil {
		log.Printf("query failed: %v", err)
	} else {
		log.Printf("id=%v, myStr='%s'\n", id, myStr)
	}
}

//nolint:testableexamples
func Example_databaseSQLBindNumericArgsOverConnector() {
	var (
		ctx          = context.TODO()
		nativeDriver = ydb.MustOpen(ctx, "grpc://localhost:2136/local")
		db           = sql.OpenDB(
			ydb.MustConnector(nativeDriver,
				ydb.WithAutoDeclare(),
				ydb.WithNumericArgs(),
			),
		)
	)
	defer nativeDriver.Close(ctx) // cleanup resources
	defer db.Close()

	// numeric args
	row := db.QueryRowContext(context.TODO(),
		"SELECT $2, $1",
		42, "my string",
	)

	var (
		id    int32  // required value
		myStr string // optional value
	)
	if err := row.Scan(&myStr, &id); err != nil {
		log.Printf("query failed: %v", err)
	} else {
		log.Printf("id=%v, myStr='%s'\n", id, myStr)
	}
}

//nolint:testableexamples
func Example_databaseSQLBindPositionalArgs() {
	db, err := sql.Open("ydb",
		"grpc://localhost:2136/local?go_query_bind=declare,positional",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }() // cleanup resources

	var (
		id    int32  // required value
		myStr string // optional value
	)

	// positional args
	row := db.QueryRowContext(context.TODO(), "SELECT ?, ?", 42, "my string")
	if err = row.Scan(&id, &myStr); err != nil {
		log.Printf("query failed: %v", err)
	} else {
		log.Printf("id=%v, myStr='%s'\n", id, myStr)
	}
}

//nolint:testableexamples
func Example_databaseSQLBindPositionalArgsOverConnector() {
	var (
		ctx          = context.TODO()
		nativeDriver = ydb.MustOpen(ctx, "grpc://localhost:2136/local")
		db           = sql.OpenDB(
			ydb.MustConnector(nativeDriver,
				ydb.WithAutoDeclare(),
				ydb.WithNumericArgs(),
			),
		)
	)
	defer nativeDriver.Close(ctx) // cleanup resources
	defer db.Close()

	// positional args
	row := db.QueryRowContext(context.TODO(), "SELECT ?, ?", 42, "my string")

	var (
		id    int32  // required value
		myStr string // optional value
	)
	if err := row.Scan(&id, &myStr); err != nil {
		log.Printf("query failed: %v", err)
	} else {
		log.Printf("id=%v, myStr='%s'\n", id, myStr)
	}
}

//nolint:testableexamples
func Example_databaseSQLBindTablePathPrefix() {
	db, err := sql.Open("ydb",
		"grpc://localhost:2136/local?go_query_bind=table_path_prefix(/local/path/to/tables)",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }() // cleanup resources

	var (
		id    int32  // required value
		title string // optional value
	)

	// full table path is "/local/path/to/tables/series"
	row := db.QueryRowContext(context.TODO(), "SELECT id, title FROM series")
	if err = row.Scan(&id, &title); err != nil {
		log.Printf("query failed: %v", err)
	} else {
		log.Printf("id=%v, title='%s'\n", id, title)
	}
}

//nolint:testableexamples
func Example_databaseSQLBindTablePathPrefixOverConnector() {
	var (
		ctx          = context.TODO()
		nativeDriver = ydb.MustOpen(ctx, "grpc://localhost:2136/local")
		db           = sql.OpenDB(ydb.MustConnector(nativeDriver,
			ydb.WithTablePathPrefix("/local/path/to/my/folder"),
		))
	)

	// full table path is "/local/path/to/tables/series"
	row := db.QueryRowContext(context.TODO(), "SELECT id, title FROM series")

	var (
		id    int32  // required value
		title string // optional value
	)
	if err := row.Scan(&id, &title); err != nil {
		log.Printf("query failed: %v", err)
	} else {
		log.Printf("id=%v, title='%s'\n", id, title)
	}
}

//nolint:testableexamples
func Example_databaseSQLBindAutoDeclare() {
	db, err := sql.Open("ydb",
		"grpc://localhost:2136/local?go_query_bind=declare",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }() // cleanup resources

	var (
		id    int32  // required value
		title string // optional value
	)

	row := db.QueryRowContext(context.TODO(), "SELECT $id, $title",
		table.ValueParam("$id", types.Uint64Value(42)),
		table.ValueParam("$title", types.TextValue("title")),
	)
	if err = row.Scan(&id, &title); err != nil {
		log.Printf("query failed: %v", err)
	} else {
		log.Printf("id=%v, title='%s'\n", id, title)
	}
}

//nolint:testableexamples
func Example_databaseSQLBindAutoDeclareOverConnector() {
	var (
		ctx          = context.TODO()
		nativeDriver = ydb.MustOpen(ctx, "grpc://localhost:2136/local")
		db           = sql.OpenDB(ydb.MustConnector(nativeDriver,
			ydb.WithAutoDeclare(),
		))
	)

	row := db.QueryRowContext(context.TODO(), "SELECT $id, $title",
		table.ValueParam("$id", types.Uint64Value(42)),
		table.ValueParam("$title", types.TextValue("title")),
	)

	var (
		id    int32  // required value
		title string // optional value
	)
	if err := row.Scan(&id, &title); err != nil {
		log.Printf("query failed: %v", err)
	} else {
		log.Printf("id=%v, title='%s'\n", id, title)
	}
}

//nolint:testableexamples
func Example_topic() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources

	reader, err := db.Topic().StartReader("consumer", topicoptions.ReadTopic("/topic/path"))
	if err != nil {
		fmt.Printf("failed start reader: %v", err)

		return
	}

	for {
		mess, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("failed start reader: %v", err)

			return
		}

		content, err := io.ReadAll(mess)
		if err != nil {
			fmt.Printf("failed start reader: %v", err)

			return
		}
		fmt.Println(string(content))
	}
}

//nolint:testableexamples
func Example_scripting() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx)                                               // cleanup resources
	if err = retry.Retry(ctx, func(ctx context.Context) (err error) { //nolint:nonamedreturns
		res, err := db.Scripting().Execute(
			ctx,
			"SELECT 1+1",
			table.NewQueryParameters(),
		)
		if err != nil {
			return err
		}
		defer res.Close() // cleanup resources
		if !res.NextResultSet(ctx) {
			return retry.RetryableError(
				fmt.Errorf("no result sets"), //nolint:err113
				retry.WithBackoff(retry.TypeNoBackoff),
			)
		}
		if !res.NextRow() {
			return retry.RetryableError(
				fmt.Errorf("no rows"), //nolint:err113
				retry.WithBackoff(retry.TypeSlowBackoff),
			)
		}
		var sum int32
		if err = res.Scan(&sum); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		if sum != 2 {
			return fmt.Errorf("unexpected sum: %v", sum) //nolint:err113
		}

		return res.Err()
	}, retry.WithIdempotent(true)); err != nil {
		fmt.Printf("Execute failed: %v", err)
	}
}

//nolint:testableexamples
func Example_discovery() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	endpoints, err := db.Discovery().Discover(ctx)
	if err != nil {
		fmt.Printf("discover failed: %v", err)

		return
	}
	fmt.Printf("%s endpoints:\n", db.Name())
	for i, e := range endpoints {
		fmt.Printf("%d) %s\n", i, e.String())
	}
}

//nolint:testableexamples
func Example_enableGzipCompressionForAllRequests() {
	ctx := context.TODO()
	db, err := ydb.Open(
		ctx,
		"grpc://localhost:2135/local",
		ydb.WithAnonymousCredentials(),
		ydb.With(config.WithGrpcOptions(
			grpc.WithDefaultCallOptions(
				grpc.UseCompressor(gzip.Name),
			),
		)),
	)
	if err != nil {
		fmt.Printf("Driver failed: %v", err)
	}
	defer db.Close(ctx) // cleanup resources
	fmt.Printf("connected to %s, database '%s'", db.Endpoint(), db.Name())
}

//nolint:testableexamples
func ExampleOpen() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2135/local")
	if err != nil {
		fmt.Printf("Driver failed: %v", err)
	}
	defer db.Close(ctx) // cleanup resources
	fmt.Printf("connected to %s, database '%s'", db.Endpoint(), db.Name())
}

//nolint:testableexamples
func ExampleOpen_advanced() {
	ctx := context.TODO()
	db, err := ydb.Open(
		ctx,
		"grpc://localhost:2135/local",
		ydb.WithAnonymousCredentials(),
		ydb.WithBalancer(
			balancers.PreferLocationsWithFallback(
				balancers.RandomChoice(), "a", "b",
			),
		),
		ydb.WithSessionPoolSizeLimit(100),
	)
	if err != nil {
		fmt.Printf("Driver failed: %v", err)
	}
	defer db.Close(ctx) // cleanup resources
	fmt.Printf("connected to %s, database '%s'", db.Endpoint(), db.Name())
}

func ExampleParamsFromMap() {
	ctx := context.TODO()
	db, err := ydb.Open(
		ctx,
		"grpc://localhost:2135/local",
		ydb.WithAnonymousCredentials(),
		ydb.WithBalancer(
			balancers.PreferLocationsWithFallback(
				balancers.RandomChoice(), "a", "b",
			),
		),
		ydb.WithSessionPoolSizeLimit(100),
	)
	if err != nil {
		fmt.Printf("Driver failed: %v", err)
	}
	defer db.Close(ctx) // cleanup resources
	fmt.Printf("connected to %s, database '%s'", db.Endpoint(), db.Name())

	res, err := db.Query().QueryRow(ctx, `
		DECLARE $textArg AS Text;
		DECLARE $intArg AS Int64;
		
		SELECT $textArg AS TextField, $intArg AS IntField
		`,
		query.WithParameters(ydb.ParamsFromMap(map[string]any{
			"$textArg": "asd",
			"$intArg":  int64(123),
		})),
	)
	if err != nil {
		fmt.Printf("query failed")
	}

	var result struct {
		TextField string
		IntField  int64
	}
	err = res.ScanStruct(&result)
	if err != nil {
		fmt.Printf("scan failed")
	}
}
