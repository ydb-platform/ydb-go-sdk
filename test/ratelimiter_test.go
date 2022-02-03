//go:build !fast
// +build !fast

package test

import (
	"context"
	"os"
	"testing"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	cfg "github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	public "github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
)

const (
	testCoordinationNodePath = "/local/test"
	testResource             = "test_res"
)

func TestRatelimiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithAnonymousCredentials(),
		ydb.With(
			config.WithRequestTimeout(time.Second*2),
			config.WithStreamTimeout(time.Second*2),
			config.WithOperationTimeout(time.Second*2),
			config.WithOperationCancelAfter(time.Second*2),
		),
		ydb.WithBalancer(balancers.PreferLocalDC( // for max tests coverage
			balancers.RandomChoice(),
		)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer t.Run("CleanupConnection", func(t *testing.T) {
		if e := db.Close(ctx); e != nil {
			t.Fatalf("db close failed: %+v", e)
		}
	})
	t.Run("DropNode", func(t *testing.T) {
		err := db.Coordination().DropNode(ctx, testCoordinationNodePath)
		if err != nil {
			if d := ydb.TransportErrorDescription(err); d != nil && d.Code() == int32(errors.TransportErrorUnimplemented) {
				return
			}
			if d := ydb.OperationErrorDescription(err); d != nil && d.Code() != int32(errors.StatusSchemeError) {
				t.Fatal(err)
			}
		}
	})
	t.Run("CreateNode", func(t *testing.T) {
		err := db.Coordination().CreateNode(ctx, testCoordinationNodePath, cfg.Config{
			Path:                     "",
			SelfCheckPeriodMillis:    1000,
			SessionGracePeriodMillis: 1000,
			ReadConsistencyMode:      cfg.ConsistencyModeRelaxed,
			AttachConsistencyMode:    cfg.ConsistencyModeRelaxed,
			RatelimiterCountersMode:  cfg.RatelimiterCountersModeDetailed,
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	defer t.Run("CleanupNode", func(t *testing.T) {
		err := db.Coordination().DropNode(ctx, testCoordinationNodePath)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("CreateResource", func(t *testing.T) {
		err := db.Ratelimiter().CreateResource(ctx, testCoordinationNodePath, public.Resource{
			ResourcePath: testResource,
			HierarchicalDrr: public.HierarchicalDrrSettings{
				MaxUnitsPerSecond:       1,
				MaxBurstSizeCoefficient: 2,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	defer t.Run("CleanupResource", func(t *testing.T) {
		err := db.Ratelimiter().DropResource(ctx, testCoordinationNodePath, testResource)
		if err != nil {
			t.Fatal("Cannot drop resource")
		}
	})
	t.Run("DescribeResource", func(t *testing.T) {
		described, err := db.Ratelimiter().DescribeResource(ctx, testCoordinationNodePath, testResource)
		if err != nil {
			t.Fatal(err)
		}
		if described == nil ||
			described.ResourcePath != testResource ||
			described.HierarchicalDrr.MaxUnitsPerSecond != 1.0 ||
			described.HierarchicalDrr.MaxBurstSizeCoefficient != 2.0 {
			t.Fatal("Resource invalid")
		}
	})
	t.Run("AlterResource", func(t *testing.T) {
		err := db.Ratelimiter().AlterResource(ctx, testCoordinationNodePath, public.Resource{
			ResourcePath: testResource,
			HierarchicalDrr: public.HierarchicalDrrSettings{
				MaxUnitsPerSecond:       3,
				MaxBurstSizeCoefficient: 4,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("CheckAlteredResource", func(t *testing.T) {
		described, err := db.Ratelimiter().DescribeResource(ctx, testCoordinationNodePath, testResource)
		if err != nil {
			t.Fatal(err)
		}
		if described == nil ||
			described.ResourcePath != testResource ||
			described.HierarchicalDrr.MaxUnitsPerSecond != 3.0 ||
			described.HierarchicalDrr.MaxBurstSizeCoefficient != 4.0 {
			t.Fatal("Resource invalid")
		}
	})
	t.Run("ListResource", func(t *testing.T) {
		list, err := db.Ratelimiter().ListResource(ctx, testCoordinationNodePath, testResource, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || list[0] != testResource {
			t.Fatal("ListResource error")
		}
	})
	t.Run("AcquireResourceAmount=<1,false>", func(t *testing.T) {
		time.Sleep(time.Second) // for accumulate
		err := db.Ratelimiter().AcquireResource(ctx, testCoordinationNodePath, testResource, 1, false)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AcquireResourceAmount=<10000,true>", func(t *testing.T) {
		err := db.Ratelimiter().AcquireResource(ctx, testCoordinationNodePath, testResource, 10000, true)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AcquireResourceAmount=<10000,false>", func(t *testing.T) {
		err := db.Ratelimiter().AcquireResource(ctx, testCoordinationNodePath, testResource, 10000, false)
		if err == nil {
			t.Fatal("Resource must not be acquired")
		}
	})
}
