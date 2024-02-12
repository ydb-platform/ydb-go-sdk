package scripting_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func Example_execute() {
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

func Example_streamExecute() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err := db.Scripting().StreamExecute(
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
				retry.WithDeleteSession(),
			)
		}
		if !res.NextRow() {
			return retry.RetryableError(
				fmt.Errorf("no rows"),
				retry.WithBackoff(retry.TypeFastBackoff),
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
		fmt.Printf("StreamExecute failed: %v", err)
	}
}

func Example_explainPlan() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	res, err := db.Scripting().Explain(
		ctx,
		"SELECT 1+1",
		scripting.ExplainModePlan,
	)
	if err != nil {
		fmt.Printf("Explain failed: %v", err)

		return
	}
	if res.Plan == "" {
		fmt.Printf("Unexpected empty plan")

		return
	}
	fmt.Printf("")
}

func Example_explainValidate() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	if err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err := db.Scripting().Explain(
			ctx,
			"SELECT 1+1",
			scripting.ExplainModeValidate,
		)
		if err != nil {
			return err
		}
		if len(res.ParameterTypes) > 0 {
			return retry.RetryableError(fmt.Errorf("unexpected parameter types"))
		}

		return nil
	}, retry.WithIdempotent(true)); err != nil {
		fmt.Printf("Explain failed: %v", err)
	}
}
