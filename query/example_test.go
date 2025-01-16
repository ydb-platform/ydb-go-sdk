//go:build go1.23

package query_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func Example_queryWithMaterializedResult() {
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
	materilizedResult, err := db.Query().Query(ctx, // context manage exiting from Do
		`SELECT 42 as id, "my string" as myStr`,
		query.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = materilizedResult.Close(ctx)
	}()

	for rs, err := range materilizedResult.ResultSets(ctx) {
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
		}
	}

	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
}

func Example_queryWithMaterializedResultSet() {
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
	materilizedResultSet, err := db.Query().QueryResultSet(ctx, // context manage exiting from Do
		`SELECT 42 as id, "my string" as myStr`,
		query.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}

	for row, err := range materilizedResultSet.Rows(ctx) {
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
	}

	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
}

func Example_queryRow() {
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
		query.WithIdempotent(),
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

func Example_explain() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		ast  string
		plan string
	)
	err = db.Query().Exec(ctx,
		`SELECT CAST(42 AS Uint32);`,
		query.WithExecMode(query.ExecModeExplain),
		query.WithStatsMode(query.StatsModeNone, func(stats query.Stats) {
			ast = stats.QueryAST()
			plan = stats.QueryPlan()
		}),
		query.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(plan)
	fmt.Println(ast)
}

func Example_withoutRangeIterators() {
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
	materializedResult, err := db.Query().Query(ctx, `SELECT 42 as id, "my string" as myStr`,
		query.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}
	for {
		rs, err := materializedResult.NextResultSet(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}
		for {
			row, err := rs.NextRow(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
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

func Example_selectWithParameters() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // required value
	)
	// Do retry operation on errors with best effort
	row, err := db.Query().QueryRow(ctx, // context manage exiting from Do
		`SELECT CAST($id AS Uint64) AS id, CAST($myStr AS Text) AS myStr`,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$id").Uint64(123).
				Param("$myStr").Text("123").
				Build(),
		),
		query.WithIdempotent(),
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

func Example_resultStats() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // required value
	)
	var stats query.Stats
	// Do retry operation on errors with best effort
	row, err := db.Query().QueryRow(ctx, // context manage exiting from Do
		`SELECT CAST($id AS Uint64) AS id, CAST($myStr AS Text) AS myStr`,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$id").Uint64(123).
				Param("$myStr").Text("123").
				Build(),
		),
		query.WithStatsMode(query.StatsModeFull, func(s query.Stats) {
			stats = s
		}),
		query.WithIdempotent(),
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
	fmt.Println("Stats:")
	fmt.Printf("- Compilation='%v'\n", stats.Compilation())
	fmt.Printf("- TotalCPUTime='%v'\n", stats.TotalCPUTime())
	fmt.Printf("- ProcessCPUTime='%v'\n", stats.ProcessCPUTime())
	fmt.Printf("- QueryAST='%v'\n", stats.QueryAST())
	fmt.Printf("- QueryPlan='%v'\n", stats.QueryPlan())
	fmt.Println("- Phases:")
	for {
		phase, ok := stats.NextPhase()
		if !ok {
			break
		}
		fmt.Printf("  - CPUTime='%v'\n", phase.CPUTime())
		fmt.Printf("  - Duration='%v'\n", phase.Duration())
		fmt.Printf("  - IsLiteralPhase='%v'\n", phase.IsLiteralPhase())
		fmt.Printf("  - AffectedShards='%v'\n", phase.AffectedShards())
		fmt.Println("  - TableAccesses:")
		for {
			tableAccess, ok := phase.NextTableAccess()
			if !ok {
				break
			}
			fmt.Printf("    - %v\n", tableAccess)
		}
	}
}

func Example_retryWithSessions() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		id    int32  // required value
		myStr string // required value
	)
	// Do retry operation on errors with best effort
	err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		streamResult, err := s.Query(ctx, // context manage exiting from Do
			`SELECT CAST($id AS Uint64) AS id, CAST($myStr AS Text) AS myStr`,
			query.WithParameters(
				ydb.ParamsBuilder().
					Param("$id").Uint64(123).
					Param("$myStr").Text("123").
					Build(),
			),
		)
		if err != nil {
			panic(err)
		}
		defer func() {
			_ = streamResult.Close(ctx)
		}()

		for rs, err := range streamResult.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				err = row.ScanNamed(
					query.Named("id", &id),
					query.Named("myStr", &myStr),
				)
				if err != nil {
					panic(err)
				}
			}
		}

		return nil
	}, query.WithIdempotent())
	if err != nil {
		panic(err)
	}

	fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
}

func Example_retryWithTx() {
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
			// for loop with ResultSets available with Go version 1.23+
			for rs, err := range res.ResultSets(ctx) {
				if err != nil {
					return err
				}
				// for loop with ResultSets available with Go version 1.23+
				for row, err := range rs.Rows(ctx) {
					if err != nil {
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

func Example_retryWithLazyTx() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local",
		ydb.WithLazyTx(true),
	)
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
			fmt.Printf("txID: expected %q, actual %q", baseTx.LazyTxID, tx.ID())
			defer func() { _ = res.Close(ctx) }() // cleanup resources
			// for loop with ResultSets available with Go version 1.23+
			for rs, err := range res.ResultSets(ctx) {
				if err != nil {
					return err
				}
				// for loop with ResultSets available with Go version 1.23+
				for row, err := range rs.Rows(ctx) {
					if err != nil {
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

func Example_executeScript() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)                      // cleanup resources
	op, err := db.Query().ExecuteScript(ctx, // context manage exiting from Do
		`SELECT CAST($id AS Uint64) AS id, CAST($myStr AS Text) AS myStr`,
		time.Hour,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$id").Uint64(123).
				Param("$myStr").Text("123").
				Build(),
		),
		query.WithIdempotent(),
	)
	if err != nil {
		panic(err)
	}

	for {
		status, err := db.Operation().Get(ctx, op.ID)
		if err != nil {
			panic(err)
		}
		if status.Ready {
			break
		}
		time.Sleep(time.Second)
	}

	var nextToken string
	for {
		result, err := db.Query().FetchScriptResults(ctx, op.ID,
			query.WithResultSetIndex(0),
			query.WithRowsLimit(1000),
			query.WithFetchToken(nextToken),
		)
		if err != nil {
			panic(err)
		}
		nextToken = result.NextToken
		for row, err := range result.ResultSet.Rows(ctx) {
			if err != nil {
				panic(err)
			}
			var (
				id    int64
				myStr string
			)
			err = row.Scan(&id, &myStr)
			if err != nil {
				panic(err)
			}
			fmt.Printf("id=%v, myStr='%s'\n", id, myStr)
		}
		if result.NextToken == "" {
			break
		}
	}
}
