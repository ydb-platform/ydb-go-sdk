package test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	cfg "github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	public "github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	testCoordinationNodePath = "/local/test"
	testResource             = "test_res"
)

func openDB(ctx context.Context) (cluster.DB, error) {
	var (
		driverTrace trace.Driver
		tableTrace  trace.Table
	)
	trace.Stub(&driverTrace, func(name string, args ...interface{}) {
		log.Printf("[driver] %s: %+v", name, trace.ClearContext(args))
	})
	trace.Stub(&tableTrace, func(name string, args ...interface{}) {
		log.Printf("[table] %s: %+v", name, trace.ClearContext(args))
	})

	db, err := ydb.New(
		ctx,
		ydb.EndpointDatabase(
			"localhost:2135",
			"/local",
			true,
		),
		ydb.WithDriverConfig(&config.Config{
			Trace:                driverTrace,
			RequestTimeout:       time.Second * 2,
			StreamTimeout:        time.Second * 2,
			OperationTimeout:     time.Second * 2,
			OperationCancelAfter: time.Second * 2,
			BalancingConfig:      config.DefaultBalancer,
		}),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
	)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func TestRateLimiter(t *testing.T) {
	t.Skip("need to be tested with docker")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	db, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	client := internal.New(db)
	coordinationClient := coordination.New(db)

	err = coordinationClient.DropNode(ctx, testCoordinationNodePath)
	if err != nil {
		t.Fatal(err)
	}
	err = coordinationClient.CreateNode(ctx, testCoordinationNodePath, cfg.Config{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      cfg.ConsistencyModeRelaxed,
		AttachConsistencyMode:    cfg.ConsistencyModeRelaxed,
		RateLimiterCountersMode:  cfg.RateLimiterCountersModeDetailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = coordinationClient.DropNode(ctx, testCoordinationNodePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = client.CreateResource(ctx, testCoordinationNodePath, public.Resource{
		ResourcePath: testResource,
		HierarchicalDrr: public.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       1,
			MaxBurstSizeCoefficient: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = client.DropResource(ctx, testCoordinationNodePath, testResource)
		if err != nil {
			t.Fatal("Cannot drop resource")
		}
	}()

	described, err := client.DescribeResource(ctx, testCoordinationNodePath, testResource)
	if err != nil {
		t.Fatal(err)
	}
	if described == nil ||
		described.ResourcePath != testResource ||
		described.HierarchicalDrr.MaxUnitsPerSecond != 1.0 ||
		described.HierarchicalDrr.MaxBurstSizeCoefficient != 2.0 {
		t.Fatal("Resource invalid")
	}

	err = client.AlterResource(ctx, testCoordinationNodePath, public.Resource{
		ResourcePath: testResource,
		HierarchicalDrr: public.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       3,
			MaxBurstSizeCoefficient: 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	described, err = client.DescribeResource(ctx, testCoordinationNodePath, testResource)
	if err != nil {
		t.Fatal(err)
	}
	if described == nil ||
		described.ResourcePath != testResource ||
		described.HierarchicalDrr.MaxUnitsPerSecond != 3.0 ||
		described.HierarchicalDrr.MaxBurstSizeCoefficient != 4.0 {
		t.Fatal("Resource invalid")
	}

	list, err := client.ListResource(ctx, testCoordinationNodePath, testResource, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0] != testResource {
		t.Fatal("ListResource error")
	}

	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 10000, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 10000, false)
	if err == nil {
		t.Fatal("Resource must not be acquired")
	}
}
