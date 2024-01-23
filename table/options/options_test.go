package options

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/feature"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var abc = "abc"

func TestSessionOptionsProfile(t *testing.T) {
	a := allocator.New()
	defer a.Free()
	{
		opt := WithProfile(
			WithProfilePreset(abc),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
		if req.Profile.PresetName != abc {
			t.Errorf("Preset is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithCompactionPolicy(WithCompactionPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
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
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
		p := req.Profile.PartitioningPolicy
		if p.PresetName != abc || p.AutoPartitioning != Ydb_Table.PartitioningPolicy_AUTO_SPLIT {
			t.Errorf("Partitioning policy is not as expected")
		}
	}
	{
		opt := WithPartitions(
			WithUniformPartitions(3),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
		if p, ok := req.Partitions.(*Ydb_Table.CreateTableRequest_UniformPartitions); !ok || p.UniformPartitions != 3 {
			t.Errorf("Uniform partitioning policy is not as expected")
		}
	}
	{
		opt := WithPartitions(
			WithExplicitPartitions(
				types.Int64Value(1),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
		p, ok := req.Partitions.(*Ydb_Table.CreateTableRequest_PartitionAtKeys)
		if !ok {
			t.Errorf("Explicitly partitioning policy is not as expected")
		} else {
			require.Equal(
				t,
				[]*Ydb.TypedValue{value.ToYDB(types.Int64Value(1), a)},
				p.PartitionAtKeys.SplitPoints,
			)
		}
	}
	{
		opt := WithProfile(
			WithExecutionPolicy(WithExecutionPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
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
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
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
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
		if req.Profile.CachingPolicy.PresetName != abc {
			t.Errorf("Caching policy is not as expected")
		}
	}
}

func TestStoragePolicyOptions(t *testing.T) {
	a := allocator.New()
	defer a.Free()
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
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
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
	a := allocator.New()
	defer a.Free()
	{
		opt := WithAddColumn("a", types.TypeBool)
		req := Ydb_Table.AlterTableRequest{}
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req), a)
		if len(req.AddColumns) != 1 ||
			req.AddColumns[0].Name != "a" {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		column := Column{
			Name:   "a",
			Type:   types.TypeBool,
			Family: "b",
		}
		opt := WithAddColumnMeta(column)
		req := Ydb_Table.AlterTableRequest{}
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req), a)
		if len(req.AddColumns) != 1 ||
			req.AddColumns[0].Name != column.Name ||
			req.AddColumns[0].Type != value.TypeToYDB(column.Type, a) ||
			req.AddColumns[0].Family != column.Family {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		opt := WithDropColumn("a")
		req := Ydb_Table.AlterTableRequest{}
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req), a)
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
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req), a)
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
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req), a)
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
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req), a)
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
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req), a)
		rrOut := NewStorageSettings(req.GetAlterStorageSettings())
		if ss != rrOut {
			t.Errorf("Alter table storage settings options is not as expected")
		}
	}
}
