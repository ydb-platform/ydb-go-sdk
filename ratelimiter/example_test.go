package ratelimiter_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
)

func Example() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	// create node
	err = db.Coordination().CreateNode(ctx, "/local/ratelimiter_test", coordination.NodeConfig{
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
	defer func() {
		// cleanup node
		err = db.Coordination().DropNode(ctx, "/local/ratelimiter_test")
		if err != nil {
			fmt.Printf("failed to drop node: %v", err)
		}
	}()
	// create resource
	err = db.Ratelimiter().CreateResource(ctx, "/local/ratelimiter_test", ratelimiter.Resource{
		ResourcePath: "test_resource",
		HierarchicalDrr: ratelimiter.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       1,
			MaxBurstSizeCoefficient: 2,
		},
	})
	if err != nil {
		fmt.Printf("failed to create resource: %v", err)
	}
	defer func() {
		// cleanup resource
		err = db.Ratelimiter().DropResource(ctx, "/local/ratelimiter_test", "test_resource")
		if err != nil {
			fmt.Printf("failed to drop resource: %v", err)
		}
	}()
	// alter resource
	err = db.Ratelimiter().AlterResource(ctx, "/local/ratelimiter_test", ratelimiter.Resource{
		ResourcePath: "test_resource",
		HierarchicalDrr: ratelimiter.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       3,
			MaxBurstSizeCoefficient: 4,
		},
	})
	if err != nil {
		fmt.Printf("failed to alter resource: %v", err)
	}
	// acquire resource amount 1
	err = db.Ratelimiter().AcquireResource(
		ctx,
		"/local/ratelimiter_test",
		"test_resource",
		1,
		ratelimiter.WithAcquire(),
	)
	if err != nil {
		fmt.Printf("failed to acquire resource: %v", err)
	}
}
