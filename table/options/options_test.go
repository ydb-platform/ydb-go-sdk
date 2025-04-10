package options

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/feature"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

var abc = "abc"

func TestSessionOptionsProfile(t *testing.T) {
	{
		opt := WithProfile(
			WithProfilePreset(abc),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		if req.GetProfile().GetPresetName() != abc {
			t.Errorf("Preset is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithCompactionPolicy(WithCompactionPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		if req.GetProfile().GetCompactionPolicy().GetPresetName() != abc {
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
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		p := req.GetProfile().GetPartitioningPolicy()
		if p.GetPresetName() != abc || p.GetAutoPartitioning() != Ydb_Table.PartitioningPolicy_AUTO_SPLIT {
			t.Errorf("Partitioning policy is not as expected")
		}
	}
	{
		opt := WithPartitions(
			WithUniformPartitions(3),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		if p, ok := req.GetPartitions().(*Ydb_Table.CreateTableRequest_UniformPartitions); !ok || p.UniformPartitions != 3 {
			t.Errorf("Uniform partitioning policy is not as expected")
		}
	}
	{
		opt := WithPartitions(
			WithExplicitPartitions(
				value.Int64Value(1),
			),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		p, ok := req.GetPartitions().(*Ydb_Table.CreateTableRequest_PartitionAtKeys)
		if !ok {
			t.Errorf("Explicitly partitioning policy is not as expected")
		} else {
			require.Equal(
				t,
				[]*Ydb.TypedValue{value.ToYDB(value.Int64Value(1))},
				p.PartitionAtKeys.GetSplitPoints(),
			)
		}
	}
	{
		opt := WithProfile(
			WithExecutionPolicy(WithExecutionPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		if req.GetProfile().GetExecutionPolicy().GetPresetName() != abc {
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
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		p := req.GetProfile().GetReplicationPolicy()
		if p.GetPresetName() != abc ||
			p.GetReplicasCount() != 3 ||
			p.GetCreatePerAvailabilityZone() != Ydb.FeatureFlag_ENABLED ||
			p.GetAllowPromotion() != Ydb.FeatureFlag_DISABLED {
			t.Errorf("Replication policy is not as expected")
		}
	}
	{
		opt := WithProfile(
			WithCachingPolicy(WithCachingPolicyPreset(abc)),
		)
		req := Ydb_Table.CreateTableRequest{}
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		if req.GetProfile().GetCachingPolicy().GetPresetName() != abc {
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
		opt.ApplyCreateTableOption((*CreateTableDesc)(&req))
		p := req.GetProfile().GetStoragePolicy()
		if p.GetPresetName() != abc ||
			p.GetSyslog().GetMedia() != "any1" ||
			p.GetLog().GetMedia() != "any2" ||
			p.GetData().GetMedia() != "any3" ||
			p.GetExternal().GetMedia() != "any4" ||
			p.GetKeepInMemory() != Ydb.FeatureFlag_ENABLED {
			t.Errorf("Storage policy is not as expected")
		}
	}
}

func TestAlterTableOptions(t *testing.T) {
	{
		opt := WithAddColumn("a", types.Bool)
		req := Ydb_Table.AlterTableRequest{}
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req))
		if len(req.GetAddColumns()) != 1 ||
			req.GetAddColumns()[0].GetName() != "a" {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		column := Column{
			Name:   "a",
			Type:   types.Bool,
			Family: "b",
		}
		opt := WithAddColumnMeta(column)
		req := Ydb_Table.AlterTableRequest{}
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req))
		if len(req.GetAddColumns()) != 1 ||
			req.GetAddColumns()[0].GetName() != column.Name ||
			req.GetAddColumns()[0].GetType() != types.TypeToYDB(column.Type) ||
			req.GetAddColumns()[0].GetFamily() != column.Family {
			t.Errorf("Alter table options is not as expected")
		}
	}
	{
		opt := WithDropColumn("a")
		req := Ydb_Table.AlterTableRequest{}
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req))
		if len(req.GetDropColumns()) != 1 ||
			req.GetDropColumns()[0] != "a" {
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
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req))
		if len(req.GetAddColumnFamilies()) != 1 ||
			req.GetAddColumnFamilies()[0].GetName() != cf.Name ||
			req.GetAddColumnFamilies()[0].GetData().GetMedia() != cf.Data.Media ||
			req.GetAddColumnFamilies()[0].GetCompression() != cf.Compression.toYDB() ||
			req.GetAddColumnFamilies()[0].GetKeepInMemory() != cf.KeepInMemory.ToYDB() {
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
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req))
		if len(req.GetAddColumnFamilies()) != 1 ||
			req.GetAddColumnFamilies()[0].GetName() != cf.Name ||
			req.GetAddColumnFamilies()[0].GetData() != nil ||
			req.GetAddColumnFamilies()[0].GetCompression() != cf.Compression.toYDB() ||
			req.GetAddColumnFamilies()[0].GetKeepInMemory() != Ydb.FeatureFlag_STATUS_UNSPECIFIED {
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
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req))
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
		opt.ApplyAlterTableOption((*AlterTableDesc)(&req))
		rrOut := NewStorageSettings(req.GetAlterStorageSettings())
		if ss != rrOut {
			t.Errorf("Alter table storage settings options is not as expected")
		}
	}
}
