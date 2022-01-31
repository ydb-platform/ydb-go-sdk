// nolint:revive
package ydb_table_options

import (
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/feature"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

var abc = "abc"

func TestSessionOptionsProfile(t *testing.T) {
	{
		opt := WithProfile(
			WithProfilePreset(abc),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*CreateTableDesc)(&req))
		if req.Profile.PresetName != abc {
			t.Errorf("Preset is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithCompactionPolicy(WithCompactionPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*CreateTableDesc)(&req))
		if req.Profile.CompactionPolicy.PresetName != abc {
			t.Errorf("Compaction policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithPartitioningPolicy(
				WithPartitioningPolicyPreset(abc),
				WithPartitioningPolicyMode(PartitioningAutoSplit),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*CreateTableDesc)(&req))
		p := req.Profile.PartitioningPolicy
		if p.PresetName != abc || p.AutoPartitioning != Ydb_Table.PartitioningPolicy_AUTO_SPLIT {
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
		opt((*CreateTableDesc)(&req))
		p := req.Profile.PartitioningPolicy
		if pp, ok := p.Partitions.(*Ydb_Table.PartitioningPolicy_UniformPartitions); !ok || pp.UniformPartitions != 3 {
			t.Errorf("Uniform partitioning policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithPartitioningPolicy(
				WithPartitioningPolicyExplicitPartitions(ydb_table_types.Int64Value(1)),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*CreateTableDesc)(&req))
		p := req.Profile.PartitioningPolicy

		pp, ok := p.Partitions.(*Ydb_Table.PartitioningPolicy_ExplicitPartitions)
		if !ok {
			t.Errorf("Explicitly partitioning policy is not as expected")
		} else {
			ydb_testutil.Equal(
				t,
				pp.ExplicitPartitions.SplitPoints,
				[]*Ydb.TypedValue{value.ToYDB(ydb_table_types.Int64Value(1))},
			)
		}
	}
	{
		opt := WithProfile(
			WithExecutionPolicy(WithExecutionPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*CreateTableDesc)(&req))
		if req.Profile.ExecutionPolicy.PresetName != abc {
			t.Errorf("Execution policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithReplicationPolicy(
				WithReplicationPolicyPreset(abc),
				WithReplicationPolicyReplicasCount(3),
				WithReplicationPolicyCreatePerAZ(FeatureEnabled),
				WithReplicationPolicyAllowPromotion(FeatureDisabled),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*CreateTableDesc)(&req))
		p := req.Profile.ReplicationPolicy
		if p.PresetName != abc ||
			p.ReplicasCount != 3 ||
			p.CreatePerAvailabilityZone != Ydb.FeatureFlag_ENABLED ||
			p.AllowPromotion != Ydb.FeatureFlag_DISABLED {
			t.Errorf("Replication policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithCachingPolicy(WithCachingPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*CreateTableDesc)(&req))
		if req.Profile.CachingPolicy.PresetName != abc {
			t.Errorf("Caching policy is not as expected")
		}
	}
}

func TestStoragePolicyOptions(t *testing.T) {
	{
		opt := WithProfile(
			WithStoragePolicy(
				WithStoragePolicyPreset(abc),
				WithStoragePolicySyslog("any1"),
				WithStoragePolicyLog("any2"),
				WithStoragePolicyData("any3"),
				WithStoragePolicyExternal("any4"),
				WithStoragePolicyKeepInMemory(FeatureEnabled),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*CreateTableDesc)(&req))
		p := req.Profile.StoragePolicy
		if p.PresetName != abc ||
			p.Syslog.Media != "any1" ||
			p.Log.Media != "any2" ||
			p.Data.Media != "any3" ||
			p.External.Media != "any4" ||
			p.KeepInMemory != Ydb.FeatureFlag_ENABLED {
			t.Errorf("Storage policy is not as expected")
		}
	}
}

func TestAlterTableOptions(t *testing.T) {
	{
		opt := WithAddColumn("a", ydb_table_types.TypeBool)
		req := Ydb_Table.AlterTableRequest{}
		opt((*AlterTableDesc)(&req))
		if len(req.AddColumns) != 1 ||
			req.AddColumns[0].Name != "a" {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		column := Column{
			Name:   "a",
			Type:   ydb_table_types.TypeBool,
			Family: "b",
		}
		opt := WithAddColumnMeta(column)
		req := Ydb_Table.AlterTableRequest{}
		opt((*AlterTableDesc)(&req))
		if len(req.AddColumns) != 1 ||
			req.AddColumns[0].Name != column.Name ||
			req.AddColumns[0].Type != value.TypeToYDB(column.Type) ||
			req.AddColumns[0].Family != column.Family {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		opt := WithDropColumn("a")
		req := Ydb_Table.AlterTableRequest{}
		opt((*AlterTableDesc)(&req))
		if len(req.DropColumns) != 1 ||
			req.DropColumns[0] != "a" {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		cf := ColumnFamily{
			Name: "a",
			Data: StoragePool{
				Media: "ssd",
			},
			Compression:  ColumnFamilyCompressionLZ4,
			KeepInMemory: FeatureEnabled,
		}
		opt := WithAlterColumnFamilies(cf)
		req := Ydb_Table.AlterTableRequest{}
		opt((*AlterTableDesc)(&req))
		if len(req.AddColumnFamilies) != 1 ||
			req.AddColumnFamilies[0].Name != cf.Name ||
			req.AddColumnFamilies[0].Data.Media != cf.Data.Media ||
			req.AddColumnFamilies[0].Compression != cf.Compression.toYDB() ||
			req.AddColumnFamilies[0].KeepInMemory != cf.KeepInMemory.ToYDB() {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		cf := ColumnFamily{
			Name:        "default",
			Compression: ColumnFamilyCompressionLZ4,
		}
		opt := WithAlterColumnFamilies(cf)
		req := Ydb_Table.AlterTableRequest{}
		opt((*AlterTableDesc)(&req))
		if len(req.AddColumnFamilies) != 1 ||
			req.AddColumnFamilies[0].Name != cf.Name ||
			req.AddColumnFamilies[0].Data != nil ||
			req.AddColumnFamilies[0].Compression != cf.Compression.toYDB() ||
			req.AddColumnFamilies[0].KeepInMemory != Ydb.FeatureFlag_STATUS_UNSPECIFIED {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		rr := ReadReplicasSettings{
			Type:  ReadReplicasAnyAzReadReplicas,
			Count: 42,
		}
		opt := WithAlterReadReplicasSettings(rr)
		req := Ydb_Table.AlterTableRequest{}
		opt((*AlterTableDesc)(&req))
		rrOut := NewReadReplicasSettings(req.GetSetReadReplicasSettings())
		if rr != rrOut {
			t.Errorf("Alter table set read replicas options is not as expected")
		}
	}
	{
		ss := StorageSettings{
			TableCommitLog0:    StoragePool{Media: "m1"},
			TableCommitLog1:    StoragePool{Media: "m2"},
			External:           StoragePool{Media: "m3"},
			StoreExternalBlobs: feature.Enabled,
		}
		opt := WithAlterStorageSettings(ss)
		req := Ydb_Table.AlterTableRequest{}
		opt((*AlterTableDesc)(&req))
		rrOut := NewStorageSettings(req.GetAlterStorageSettings())
		if ss != rrOut {
			t.Errorf("Alter table storage settings options is not as expected")
		}
	}
}
