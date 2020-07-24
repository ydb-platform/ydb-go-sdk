package table

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Table"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
)

func TestSessionOptionsProfile(t *testing.T) {
	{
		opt := WithProfile(
			WithProfilePreset("abc"),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		if req.Profile.PresetName != "abc" {
			t.Errorf("Preset is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithStoragePolicy(
				WithStoragePolicyPreset("abc"),
				WithStoragePolicySyslog("any1"),
				WithStoragePolicyLog("any2"),
				WithStoragePolicyData("any3"),
				WithStoragePolicyExternal("any4"),
				WithStoragePolicyKeepInMemory(ydb.FeatureEnabled),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		p := req.Profile.StoragePolicy
		if p.PresetName != "abc" ||
			p.Syslog.Media != "any1" ||
			p.Log.Media != "any2" ||
			p.Data.Media != "any3" ||
			p.External.Media != "any4" ||
			p.KeepInMemory != Ydb.FeatureFlag_ENABLED {
			t.Errorf("Storage policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithCompactionPolicy(WithCompactionPolicyPreset("abc")),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		if req.Profile.CompactionPolicy.PresetName != "abc" {
			t.Errorf("Compaction policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithPartitioningPolicy(
				WithPartitioningPolicyPreset("abc"),
				WithPartitioningPolicyMode(PartitioningAutoSplit),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		p := req.Profile.PartitioningPolicy
		if p.PresetName != "abc" || p.AutoPartitioning != Ydb_Table.PartitioningPolicy_AUTO_SPLIT {
			t.Errorf("Partitioning policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithPartitioningPolicy(
				WithPartitioningPolicyUniformPartitions(3),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		p := req.Profile.PartitioningPolicy
		if pp, ok := p.Partitions.(*Ydb_Table.PartitioningPolicy_UniformPartitions); !ok || pp.UniformPartitions != 3 {
			t.Errorf("Uniform partitioning policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithPartitioningPolicy(
				WithPartitioningPolicyExplicitPartitions(ydb.Int64Value(1)),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		p := req.Profile.PartitioningPolicy

		if pp, ok := p.Partitions.(*Ydb_Table.PartitioningPolicy_ExplicitPartitions); !ok || !cmp.Equal(pp.ExplicitPartitions.SplitPoints, []*Ydb.TypedValue{internal.ValueToYDB(ydb.Int64Value(1))}, cmp.Comparer(proto.Equal)) {
			t.Errorf("Explicitly partitioning policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithExecutionPolicy(WithExecutionPolicyPreset("abc")),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		if req.Profile.ExecutionPolicy.PresetName != "abc" {
			t.Errorf("Execution policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithReplicationPolicy(
				WithReplicationPolicyPreset("abc"),
				WithReplicationPolicyReplicasCount(3),
				WithReplicationPolicyCreatePerAZ(ydb.FeatureEnabled),
				WithReplicationPolicyAllowPromotion(ydb.FeatureDisabled),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		p := req.Profile.ReplicationPolicy
		if p.PresetName != "abc" ||
			p.ReplicasCount != 3 ||
			p.CreatePerAvailabilityZone != Ydb.FeatureFlag_ENABLED ||
			p.AllowPromotion != Ydb.FeatureFlag_DISABLED {
			t.Errorf("Replication policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithCachingPolicy(WithCachingPolicyPreset("abc")),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*createTableDesc)(&req))
		if req.Profile.CachingPolicy.PresetName != "abc" {
			t.Errorf("Caching policy is not as expected")
		}
	}

}
