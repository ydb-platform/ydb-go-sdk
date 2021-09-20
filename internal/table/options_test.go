package table

import (
	options2 "github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
)

var (
	abc = "abc"
)

func TestSessionOptionsProfile(t *testing.T) {
	{
		opt := options2.WithProfile(
			options2.WithProfilePreset(abc),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
		if req.Profile.PresetName != abc {
			t.Errorf("Preset is not as expected")
		}
	}
	{
		opt := options2.WithProfile(
			options2.WithCompactionPolicy(options2.WithCompactionPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
		if req.Profile.CompactionPolicy.PresetName != abc {
			t.Errorf("Compaction policy is not as expected")
		}
	}
	{
		opt := options2.WithProfile(
			options2.WithPartitioningPolicy(
				options2.WithPartitioningPolicyPreset(abc),
				options2.WithPartitioningPolicyMode(PartitioningAutoSplit),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
		p := req.Profile.PartitioningPolicy
		if p.PresetName != abc || p.AutoPartitioning != Ydb_Table.PartitioningPolicy_AUTO_SPLIT {
			t.Errorf("Partitioning policy is not as expected")
		}
	}
	{
		opt := options2.WithProfile(
			options2.WithPartitioningPolicy(
				options2.WithPartitioningPolicyUniformPartitions(3),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
		p := req.Profile.PartitioningPolicy
		if pp, ok := p.Partitions.(*Ydb_Table.PartitioningPolicy_UniformPartitions); !ok || pp.UniformPartitions != 3 {
			t.Errorf("Uniform partitioning policy is not as expected")
		}
	}
	{
		opt := options2.WithProfile(
			options2.WithPartitioningPolicy(
				options2.WithPartitioningPolicyExplicitPartitions(types.Int64Value(1)),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
		p := req.Profile.PartitioningPolicy

		pp, ok := p.Partitions.(*Ydb_Table.PartitioningPolicy_ExplicitPartitions)
		if !ok {
			t.Errorf("Explicitly partitioning policy is not as expected")
		} else {
			internal.Equal(t, pp.ExplicitPartitions.SplitPoints, []*Ydb.TypedValue{internal.ValueToYDB(types.Int64Value(1))})
		}
	}
	{
		opt := options2.WithProfile(
			options2.WithExecutionPolicy(options2.WithExecutionPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
		if req.Profile.ExecutionPolicy.PresetName != abc {
			t.Errorf("Execution policy is not as expected")
		}
	}
	{
		opt := options2.WithProfile(
			options2.WithReplicationPolicy(
				options2.WithReplicationPolicyPreset(abc),
				options2.WithReplicationPolicyReplicasCount(3),
				options2.WithReplicationPolicyCreatePerAZ(options2.FeatureEnabled),
				options2.WithReplicationPolicyAllowPromotion(options2.FeatureDisabled),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
		p := req.Profile.ReplicationPolicy
		if p.PresetName != abc ||
			p.ReplicasCount != 3 ||
			p.CreatePerAvailabilityZone != Ydb.FeatureFlag_ENABLED ||
			p.AllowPromotion != Ydb.FeatureFlag_DISABLED {
			t.Errorf("Replication policy is not as expected")
		}
	}
	{
		opt := options2.WithProfile(
			options2.WithCachingPolicy(options2.WithCachingPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
		if req.Profile.CachingPolicy.PresetName != abc {
			t.Errorf("Caching policy is not as expected")
		}
	}
}

func TestStoragePolicyOptions(t *testing.T) {
	{
		opt := options2.WithProfile(
			options2.WithStoragePolicy(
				options2.WithStoragePolicyPreset(abc),
				options2.WithStoragePolicySyslog("any1"),
				options2.WithStoragePolicyLog("any2"),
				options2.WithStoragePolicyData("any3"),
				options2.WithStoragePolicyExternal("any4"),
				options2.WithStoragePolicyKeepInMemory(options2.FeatureEnabled),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt((*scanner.createTableDesc)(&req))
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
		opt := WithAddColumn("a", types.TypeBool)
		req := Ydb_Table.AlterTableRequest{}
		opt((*alterTableDesc)(&req))
		if len(req.AddColumns) != 1 ||
			req.AddColumns[0].Name != "a" {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		column := options2.Column{
			Name:   "a",
			Type:   types.TypeBool,
			Family: "b",
		}
		opt := WithAddColumnMeta(column)
		req := Ydb_Table.AlterTableRequest{}
		opt((*alterTableDesc)(&req))
		if len(req.AddColumns) != 1 ||
			req.AddColumns[0].Name != column.Name ||
			req.AddColumns[0].Type != internal.TypeToYDB(column.Type) ||
			req.AddColumns[0].Family != column.Family {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		opt := WithDropColumn("a")
		req := Ydb_Table.AlterTableRequest{}
		opt((*alterTableDesc)(&req))
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
			KeepInMemory: options2.FeatureEnabled,
		}
		opt := WithAlterColumnFamilies(cf)
		req := Ydb_Table.AlterTableRequest{}
		opt((*alterTableDesc)(&req))
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
		opt((*alterTableDesc)(&req))
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
		opt((*alterTableDesc)(&req))
		rrOut := readReplicasSettings(req.GetSetReadReplicasSettings())
		if rr != rrOut {
			t.Errorf("Alter table set read replicas options is not as expected")
		}
	}
	{
		ss := StorageSettings{
			TableCommitLog0:    StoragePool{Media: "m1"},
			TableCommitLog1:    StoragePool{Media: "m2"},
			External:           StoragePool{Media: "m3"},
			StoreExternalBlobs: internal.FeatureEnabled,
		}
		opt := WithAlterStorageSettings(ss)
		req := Ydb_Table.AlterTableRequest{}
		opt((*alterTableDesc)(&req))
		rrOut := storageSettings(req.GetAlterStorageSettings())
		if ss != rrOut {
			t.Errorf("Alter table storage settings options is not as expected")
		}
	}
}
