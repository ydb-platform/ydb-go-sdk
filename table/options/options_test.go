package options

import (
	"testing"

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
	presetName := "abc"

	testProfilePreset(t, a, presetName)
	testCompactionPolicy(t, a, presetName)
	testPartitioningPolicy(t, a, presetName, PartitioningAutoSplit)
	testExecutionPolicy(t, a, presetName)
	testReplicationPolicy(t, a, presetName, 3, FeatureEnabled, FeatureDisabled)
	testCachingPolicy(t, a, presetName)
}

func testProfilePreset(t *testing.T, a *allocator.Allocator, presetName string) {
	opt := WithProfile(WithProfilePreset(presetName))
	req := Ydb_Table.CreateTableRequest{}
	opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
	if req.Profile.PresetName != presetName {
		t.Errorf("Preset is not as expected")
	}
}

func testCompactionPolicy(t *testing.T, a *allocator.Allocator, presetName string) {
	opt := WithProfile(WithCompactionPolicy(WithCompactionPolicyPreset(presetName)))
	req := Ydb_Table.CreateTableRequest{}
	opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
	if req.Profile.CompactionPolicy.PresetName != presetName {
		t.Errorf("Compaction policy is not as expected")
	}
}

func testPartitioningPolicy(t *testing.T, a *allocator.Allocator, presetName string, mode PartitioningMode) {
	opt := WithProfile(
		WithPartitioningPolicy(
			WithPartitioningPolicyPreset(presetName),
			WithPartitioningPolicyMode(mode),
		),
	)
	req := Ydb_Table.CreateTableRequest{}
	opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
	p := req.Profile.PartitioningPolicy
	if p.PresetName != presetName || p.AutoPartitioning != mode.toYDB() {
		t.Errorf("Partitioning policy is not as expected")
	}
}

func testExecutionPolicy(t *testing.T, a *allocator.Allocator, presetName string) {
	opt := WithProfile(WithExecutionPolicy(WithExecutionPolicyPreset(presetName)))
	req := Ydb_Table.CreateTableRequest{}
	opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
	if req.Profile.ExecutionPolicy.PresetName != presetName {
		t.Errorf("Execution policy is not as expected")
	}
}

func testReplicationPolicy(t *testing.T, a *allocator.Allocator, presetName string, replicasCount uint32, perAZ, allowPromotion FeatureFlag) {
	opt := WithProfile(
		WithReplicationPolicy(
			WithReplicationPolicyPreset(presetName),
			WithReplicationPolicyReplicasCount(replicasCount),
			WithReplicationPolicyCreatePerAZ(perAZ),
			WithReplicationPolicyAllowPromotion(allowPromotion),
		),
	)
	req := Ydb_Table.CreateTableRequest{}
	opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
	p := req.Profile.ReplicationPolicy
	if p.PresetName != presetName ||
		p.ReplicasCount != uint32(replicasCount) ||
		p.CreatePerAvailabilityZone != convertFeatureFlagToYDB(perAZ) ||
		p.AllowPromotion != convertFeatureFlagToYDB(allowPromotion) {

		t.Errorf("Replication policy is not as expected: %+v", p)
	}
}

func convertFeatureFlagToYDB(flag FeatureFlag) Ydb.FeatureFlag_Status {
	switch flag {
	case FeatureEnabled:
		return Ydb.FeatureFlag_ENABLED
	case FeatureDisabled:
		return Ydb.FeatureFlag_DISABLED
	default:
		return Ydb.FeatureFlag_STATUS_UNSPECIFIED
	}
}

func testCachingPolicy(t *testing.T, a *allocator.Allocator, presetName string) {
	opt := WithProfile(WithCachingPolicy(WithCachingPolicyPreset(presetName)))
	req := Ydb_Table.CreateTableRequest{}
	opt.ApplyCreateTableOption((*CreateTableDesc)(&req), a)
	if req.Profile.CachingPolicy.PresetName != presetName {
		t.Errorf("Caching policy is not as expected")
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
