package query_test

import (
	"context"
	"errors"
	"fmt"
	"io"

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
		func(ctx context.Context, s query.Session) error { // retry operation
			var errIn error
			_, res, errIn := s.Execute(ctx,
				`SELECT 42 as id, "my string" as myStr`,
			)
			if errIn != nil {
				return errIn // for auto-retry with driver
			}
			defer func() { _ = res.Close(ctx) }() // cleanup resources
			for {                                 // iterate over result sets
				rs, errIn := res.NextResultSet(ctx)
				if errIn != nil {
					if errors.Is(errIn, io.EOF) {
						break
					}

					return errIn
				}
				for { // iterate over rows
					row, errIn := rs.NextRow(ctx)
					if errIn != nil {
						if errors.Is(errIn, io.EOF) {
							break
						}

						return errIn
					}
					if errIn = row.Scan(&id, &myStr); err != nil {
						return errIn // generally scan error not retryable, return it for driver check error
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
		func(ctx context.Context, s query.Session) error { // retry operation
			_, res, errIn := s.Execute(ctx,
				`SELECT CAST($id AS Uint64) AS id, CAST($myStr AS Text) AS myStr`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(123).
						Param("$myStr").Text("123").
						Build(),
				),
			)
			if errIn != nil {
				return errIn // for auto-retry with driver
			}
			defer func() { _ = res.Close(ctx) }() // cleanup resources
			for {                                 // iterate over result sets
				rs, errIn := res.NextResultSet(ctx)
				if errIn != nil {
					if errors.Is(errIn, io.EOF) {
						break
					}

					return errIn
				}
				for { // iterate over rows
					row, errIn := rs.NextRow(ctx)
					if errIn != nil {
						if errors.Is(errIn, io.EOF) {
							break
						}

						return errIn
					}
					if errIn = row.ScanNamed(
						query.Named("id", &id),
						query.Named("myStr", &myStr),
					); errIn != nil {
						return errIn // generally scan error not retryable, return it for driver check error
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
		func(ctx context.Context, tx query.TxActor) error { // retry operation
			res, errIn := tx.Execute(ctx,
				`SELECT 42 as id, "my string" as myStr`,
			)
			if errIn != nil {
				return errIn // for auto-retry with driver
			}
			defer func() { _ = res.Close(ctx) }() // cleanup resources
			for {                                 // iterate over result sets
				rs, errIn := res.NextResultSet(ctx)
				if errIn != nil {
					if errors.Is(errIn, io.EOF) {
						break
					}

					return errIn
				}
				for { // iterate over rows
					row, errIn := rs.NextRow(ctx)
					if errIn != nil {
						if errors.Is(errIn, io.EOF) {
							break
						}

						return errIn
					}
					if errIn = row.ScanNamed(
						query.Named("id", &id),
						query.Named("myStr", &myStr),
					); errIn != nil {
						return errIn // generally scan error not retryable, return it for driver check error
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
