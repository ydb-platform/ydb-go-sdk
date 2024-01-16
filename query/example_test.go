package query_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func Example_selectWithoutParameters() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // optional value
	)
	// Do retry operation on errors with best effort
	err = db.Query().Do(ctx, // context manage exiting from Do
		func(ctx context.Context, s query.Session) (err error) { // retry operation
			_, res, err := s.Execute(ctx,
				`SELECT 42 as id, "my string" as myStr`,
			)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer func() { _ = res.Close() }() // cleanup resources
			for res.NextResultSet(ctx) {       // iterate over result sets
				for res.Next() { // iterate over rows
					if err = res.Scan(&id, &myStr); err != nil {
						return err // generally scan error not retryable, return it for driver check error
					}
				}
			}
			return res.Err() // return finally result error for auto-retry with driver
		},
		query.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
}

func Example_selectWithParameters() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // optional value
	)
	// Do retry operation on errors with best effort
	err = db.Query().Do(ctx, // context manage exiting from Do
		func(ctx context.Context, s query.Session) (err error) { // retry operation
			_, res, err := s.Execute(ctx,
				`SELECT CAST($id AS Uint64) AS id, CAST($myStr AS Text) AS myStr`,
				query.WithParameters(
					query.Param("$id", query.Uint64Value(123)),
					query.Param("$myStr", query.TextValue("test")),
				),
			)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer func() { _ = res.Close() }() // cleanup resources
			for res.NextResultSet(ctx) {       // iterate over result sets
				for res.Next() { // iterate over rows
					err = res.ScanNamed(
						query.Named("id", &id),
						query.Named("myStr", &myStr),
					)
					if err != nil {
						return err // generally scan error not retryable, return it for driver check error
					}
				}
			}
			return res.Err() // return finally result error for auto-retry with driver
		},
		query.WithIdempotent(),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
}

func Example_txSelect() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // optional value
	)
	// Do retry operation on errors with best effort
	err = db.Query().DoTx(ctx, // context manage exiting from Do
		func(ctx context.Context, tx query.TransactionActor) (err error) { // retry operation
			res, err := tx.Execute(ctx,
				`SELECT 42 as id, "my string" as myStr`,
			)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer func() { _ = res.Close() }() // cleanup resources
			for res.NextResultSet(ctx) {       // iterate over result sets
				for res.Next() { // iterate over rows
					err = res.ScanNamed(
						query.Named("id", &id),
						query.Named("myStr", &myStr),
					)
					if err != nil {
						return err // generally scan error not retryable, return it for driver check error
					}
				}
			}
			return res.Err() // return finally result error for auto-retry with driver
		},
		query.WithIdempotent(),
		query.WithTxSettings(query.TxSettings(
			query.WithSnapshotReadOnly(),
		)),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
}
