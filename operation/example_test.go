package operation_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/operation"
)

func Example_listOperations() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	var nextToken string
	for i := 0; ; i++ {
		operations, err := db.Operation().List(ctx, "buildindex",
			operation.WithPageSize(10),
			operation.WithPageToken(nextToken),
		)
		if err != nil {
			panic(err)
		}
		nextToken = operations.NextToken
		fmt.Printf("page#%d. operations:\n", i)
		for _, op := range operations.Operations {
			fmt.Println(" -", op)
		}
		if len(operations.Operations) == 0 {
			break
		}
	}
}
