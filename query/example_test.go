//go:build go1.23

package query_test

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func Example_readRow() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // optional value
	)
	// Do retry operation on errors with best effort
	row, err := db.Query().QueryRow(ctx, // context manage exiting from Do
		`SELECT 42 as id, "my string" as myStr`,
	)
	if err != nil {
		panic(err)
	}

	err = row.ScanNamed(
		query.Named("id", &id),
		query.Named("myStr", &myStr),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
}

func Example_rangeWithLegacyGo() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // optional value
	)
	r, err := db.Query().Query(ctx, `SELECT 42 as id, "my string" as myStr`)
	if err != nil {
		panic(err)
	}
	for rs, err := range r.ResultSets(ctx) {
		if err != nil {
			panic(err)
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				panic(err)
			}
			err = row.ScanNamed(
				query.Named("id", &id),
				query.Named("myStr", &myStr),
			)
			if err != nil {
				panic(err)
			}

			fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
		}
	}
}

func Example_rangeExperiment() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // optional value
	)
	r, err := db.Query().Query(ctx, `SELECT 42 as id, "my string" as myStr`)
	if err != nil {
		panic(err)
	}
	// for loop with ResultSets available with Go version 1.23+
	for rs, err := range r.ResultSets(ctx) {
		if err != nil {
			panic(err)
		}
		// for loop with ResultSets available with Go version 1.23+
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				panic(err)
			}
			err = row.ScanNamed(
				query.Named("id", &id),
				query.Named("myStr", &myStr),
			)
			if err != nil {
				panic(err)
			}

			fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
		}
	}
}

func Example_selectWithoutParameters() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // optional value
	)
	// Do retry operation on errors with best effort
	err = db.Query().Do(ctx, // context manage exiting from Do
		func(ctx context.Context, s query.Session) (err error) { // retry operation
			res, err := s.Query(ctx,
				`SELECT 42 as id, "my string" as myStr`,
			)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer func() { _ = res.Close(ctx) }() // cleanup resources
			for {                                 // iterate over result sets
				rs, err := res.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}
				for { // iterate over rows
					row, err := rs.NextRow(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						return err
					}
					if err = row.Scan(&id, &myStr); err != nil {
						return err // generally scan error not retryable, return it for driver check error
					}
				}
			}

			return nil
		},
		query.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
	// id=42, myStr='my string'
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
			res, err := s.Query(ctx,
				`SELECT CAST($id AS Uint64) AS id, CAST($myStr AS Text) AS myStr`,
				options.WithParameters(
					ydb.ParamsBuilder().
						Param("$id").Uint64(123).
						Param("$myStr").Text("123").
						Build(),
				),
			)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer func() { _ = res.Close(ctx) }() // cleanup resources
			for {                                 // iterate over result sets
				rs, err := res.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}
				for { // iterate over rows
					row, err := rs.NextRow(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						return err
					}
					if err = row.ScanNamed(
						query.Named("id", &id),
						query.Named("myStr", &myStr),
					); err != nil {
						return err // generally scan error not retryable, return it for driver check error
					}
				}
			}

			return nil
		},
		options.WithIdempotent(),
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
		func(ctx context.Context, tx query.TxActor) (err error) { // retry operation
			res, err := tx.Query(ctx,
				`SELECT 42 as id, "my string" as myStr`,
			)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer func() { _ = res.Close(ctx) }() // cleanup resources
			for {                                 // iterate over result sets
				rs, err := res.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}
				for { // iterate over rows
					row, err := rs.NextRow(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						return err
					}
					if err = row.ScanNamed(
						query.Named("id", &id),
						query.Named("myStr", &myStr),
					); err != nil {
						return err // generally scan error not retryable, return it for driver check error
					}
				}
			}

			return nil
		},
		options.WithIdempotent(),
		options.WithTxSettings(query.TxSettings(
			query.WithSnapshotReadOnly(),
		)),
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
}
