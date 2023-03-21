package ydb_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

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

func Example_databaseSQL() {
	db, err := sql.Open("ydb", "grpc://localhost:2136/local")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }() // cleanup resources

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
	}, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		log.Printf("query failed: %v", err)
	}
}

func Example_databaseSQLBindNumericArgs() {
	db, err := sql.Open("ydb",
		"grpc://localhost:2136/local?go_auto_bind=declare,numeric",
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

func Example_databaseSQLBindNumericArgsOverConnector() {
	var (
		ctx          = context.TODO()
		nativeDriver = ydb.MustOpen(ctx, "grpc://localhost:2136/local")
		db           = sql.OpenDB(
			ydb.MustConnector(nativeDriver,
				ydb.WithAutoBind(
					query.Origin(),
					query.TablePathPrefix("/local/path/to/my/folder"),
					query.Declare(),
					query.Numeric(),
				),
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

func Example_databaseSQLBindPositionalArgs() {
	db, err := sql.Open("ydb",
		"grpc://localhost:2136/local?go_auto_bind=declare,positional",
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

func Example_databaseSQLBindPositionalArgsOverConnector() {
	var (
		ctx          = context.TODO()
		nativeDriver = ydb.MustOpen(ctx, "grpc://localhost:2136/local")
		db           = sql.OpenDB(
			ydb.MustConnector(nativeDriver,
				ydb.WithAutoBind(
					query.Origin(),
					query.TablePathPrefix("/local/path/to/my/folder"),
					query.Declare(),
					query.Numeric(),
				),
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

func Example_databaseSQLBindTablePathPrefix() {
	db, err := sql.Open("ydb",
		"grpc://localhost:2136/local?go_auto_bind=table_path_prefix&go_auto_bind.table_path_prefix=path/to/tables",
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

func Example_databaseSQLBindTablePathPrefixOverConnector() {
	var (
		ctx          = context.TODO()
		nativeDriver = ydb.MustOpen(ctx, "grpc://localhost:2136/local")
		db           = sql.OpenDB(
			ydb.MustConnector(nativeDriver,
				ydb.WithAutoBind(
					query.TablePathPrefix("/local/path/to/my/folder"),
				),
			),
		)
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

func Example_databaseSQLBindDeclare() {
	db, err := sql.Open("ydb",
		"grpc://localhost:2136/local?go_auto_bind=declare",
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

func Example_databaseSQLBindDeclareOverConnector() {
	var (
		ctx          = context.TODO()
		nativeDriver = ydb.MustOpen(ctx, "grpc://localhost:2136/local")
		db           = sql.OpenDB(
			ydb.MustConnector(nativeDriver,
				ydb.WithAutoBind(
					query.Declare(),
				),
			),
		)
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

func Example_scripting() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
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
				fmt.Errorf("no result sets"),
				retry.WithBackoff(retry.TypeNoBackoff),
			)
		}
		if !res.NextRow() {
			return retry.RetryableError(
				fmt.Errorf("no rows"),
				retry.WithBackoff(retry.TypeSlowBackoff),
			)
		}
		var sum int32
		if err = res.Scan(&sum); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		if sum != 2 {
			return fmt.Errorf("unexpected sum: %v", sum)
		}
		return res.Err()
	}, retry.WithIdempotent(true)); err != nil {
		fmt.Printf("Execute failed: %v", err)
	}
}

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

func ExampleOpen() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2135/local")
	if err != nil {
		fmt.Printf("Driver failed: %v", err)
	}
	defer db.Close(ctx) // cleanup resources
	fmt.Printf("connected to %s, database '%s'", db.Endpoint(), db.Name())
}

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

func TestWithAutoBind(t *testing.T) {
	bindings := query.NewBind(
		query.TablePathPrefix("/local/path/to/my/folder"), // bind pragma TablePathPrefix
		query.Declare(),    // bind parameters declare
		query.Positional(), // auto-replace of positional args
	)
	query, params, err := bindings.ToYQL("SELECT ?, ?, ?", 1, uint64(2), "3")
	require.NoError(t, err)
	require.Equal(t, `-- bind TablePathPrefix
PRAGMA TablePathPrefix("/local/path/to/my/folder");

-- bind declares
DECLARE $p0 AS Int32;
DECLARE $p1 AS Uint64;
DECLARE $p2 AS Utf8;

-- origin query with positional args replacement
SELECT $p0, $p1, $p2`, query)
	require.Equal(t, table.NewQueryParameters(
		table.ValueParam("$p0", types.Int32Value(1)),
		table.ValueParam("$p1", types.Uint64Value(2)),
		table.ValueParam("$p2", types.TextValue("3")),
	), params)
}
