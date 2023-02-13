package ydb_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func Example_table() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
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
	db, err := sql.Open("ydb", "grpcs://localhost:2135/local")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }() // cleanup resources

	var (
		query = `SELECT 42 as id, "my string" as myStr`
		id    int32  // required value
		myStr string // optional value
	)
	err = retry.DoTx(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, query)
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

func Example_topic() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
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

		content, err := ioutil.ReadAll(mess)
		if err != nil {
			fmt.Printf("failed start reader: %v", err)
			return
		}
		fmt.Println(string(content))
	}
}

func Example_scripting() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
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
	db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
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
		fmt.Printf("connection failed: %v", err)
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
		fmt.Printf("connection failed: %v", err)
	}
	defer db.Close(ctx) // cleanup resources
	fmt.Printf("connected to %s, database '%s'", db.Endpoint(), db.Name())
}
