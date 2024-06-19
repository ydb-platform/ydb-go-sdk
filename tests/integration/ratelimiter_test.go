//go:build integration
// +build integration

package integration

import (
	"os"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestRatelimiter(sourceTest *testing.T) {
	t := xtest.MakeSyncedTest(sourceTest)
	const (
		testCoordinationNodePath = "/local/ratelimiter_test"
		testResource             = "test_resource"
	)

	ctx := xtest.Context(t)

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
		ydb.With(
			config.WithOperationTimeout(time.Second*2),
			config.WithOperationCancelAfter(time.Second*2),
		),
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithLogger(
			newLoggerWithMinLevel(t, log.WARN),
			trace.MatchDetails(`ydb\.(driver|discovery|retry|ratelimiter|coordination).*`),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// cleanup connection
		if e := db.Close(ctx); e != nil {
			t.Fatalf("db close failed: %+v", e)
		}
	}()
	// drop node
	err = db.Coordination().DropNode(ctx, testCoordinationNodePath)
	if err != nil {
		if !ydb.IsOperationErrorSchemeError(err) {
			t.Fatal(err)
		}
	}
	// create node
	err = db.Coordination().CreateNode(ctx, testCoordinationNodePath, coordination.NodeConfig{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      coordination.ConsistencyModeRelaxed,
		AttachConsistencyMode:    coordination.ConsistencyModeRelaxed,
		RatelimiterCountersMode:  coordination.RatelimiterCountersModeDetailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// cleanup node
		err = db.Coordination().DropNode(ctx, testCoordinationNodePath)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// create resource
	err = db.Ratelimiter().CreateResource(ctx, testCoordinationNodePath, ratelimiter.Resource{
		ResourcePath: testResource,
		HierarchicalDrr: ratelimiter.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       1,
			MaxBurstSizeCoefficient: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// cleanup resource
		err = db.Ratelimiter().DropResource(ctx, testCoordinationNodePath, testResource)
		if err != nil {
			t.Fatalf("Cannot drop resource: %v", err)
		}
	}()
	// describe resource
	described, err := db.Ratelimiter().DescribeResource(ctx, testCoordinationNodePath, testResource)
	if err != nil {
		t.Fatal(err)
	}
	if described == nil ||
		described.ResourcePath != testResource ||
		described.HierarchicalDrr.MaxUnitsPerSecond != 1.0 ||
		described.HierarchicalDrr.MaxBurstSizeCoefficient != 2.0 {
		t.Fatalf("Resource invalid: %+v", described)
	}
	// alter resource
	err = db.Ratelimiter().AlterResource(ctx, testCoordinationNodePath, ratelimiter.Resource{
		ResourcePath: testResource,
		HierarchicalDrr: ratelimiter.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       3,
			MaxBurstSizeCoefficient: 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// check altered resource
	described, err = db.Ratelimiter().DescribeResource(ctx, testCoordinationNodePath, testResource)
	if err != nil {
		t.Fatal(err)
	}
	if described == nil ||
		described.ResourcePath != testResource ||
		described.HierarchicalDrr.MaxUnitsPerSecond != 3.0 ||
		described.HierarchicalDrr.MaxBurstSizeCoefficient != 4.0 {
		t.Fatal("Resource invalid")
	}
	// list resource
	list, err := db.Ratelimiter().ListResource(
		ctx,
		testCoordinationNodePath,
		testResource,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0] != testResource {
		t.Fatal("ListResource error")
	}
	// acquire resource amount 1
	time.Sleep(time.Second) // for accumulate
	err = db.Ratelimiter().AcquireResource(
		ctx,
		testCoordinationNodePath,
		testResource,
		1,
		ratelimiter.WithAcquire(),
	)
	if err != nil {
		t.Fatal(err)
	}
	// report resource amount 10000
	err = db.Ratelimiter().AcquireResource(
		ctx,
		testCoordinationNodePath,
		testResource,
		10000,
		ratelimiter.WithReport(),
	)
	if err != nil {
		t.Fatal(err)
	}
	// acquire resource amount 10000
	err = db.Ratelimiter().AcquireResource(
		ctx,
		testCoordinationNodePath,
		testResource,
		10000,
		ratelimiter.WithAcquire(),
	)
	if err == nil {
		t.Fatal("Resource must not be acquired")
	}
}
