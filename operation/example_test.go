package operation_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func Example_listOperations() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	operations, err := db.Operation().ListBuildIndex(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("operations:\n")
	for _, op := range operations.Operations {
		fmt.Printf(" - operation %q:", op.ID)
		fmt.Printf("   - ConsumedUnits %f", op.ConsumedUnits)
		fmt.Printf("   - Ready %b", op.Ready)
		fmt.Printf("   - Status %q", op.Status)
		fmt.Printf("   - State %q", op.Metadata.State)
		fmt.Printf("   - Description %q", op.Metadata.Description)
		fmt.Printf("   - Progress %f", op.Metadata.Progress)
	}
}
