package coordination_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
)

//nolint:errcheck
func Example() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)
		return
	}
	defer db.Close(ctx) // cleanup resources
	// create node
	err = db.Coordination().CreateNode(ctx, "/local/test", coordination.NodeConfig{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      coordination.ConsistencyModeRelaxed,
		AttachConsistencyMode:    coordination.ConsistencyModeRelaxed,
		RatelimiterCountersMode:  coordination.RatelimiterCountersModeDetailed,
	})
	if err != nil {
		fmt.Printf("failed to create node: %v", err)
		return
	}
	defer db.Coordination().DropNode(ctx, "/local/test")
	e, c, err := db.Coordination().DescribeNode(ctx, "/local/test")
	if err != nil {
		fmt.Printf("failed to describe node: %v", err)
		return
	}
	fmt.Printf("node description: %+v\nnode config: %+v\n", e, c)
}
