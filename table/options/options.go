package options

import (
	"bytes"
	"compress/gzip"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func WithShardKeyBounds() DescribeTableOption {
	return func(d *DescribeTableDesc) {
		(*Ydb_Table.DescribeTableRequest)(d).SetIncludeShardKeyBounds(true)
	}
}

func WithTableStats() DescribeTableOption {
	return func(d *DescribeTableDesc) {
		(*Ydb_Table.DescribeTableRequest)(d).SetIncludeTableStats(true)
	}
}

func WithPartitionStats() DescribeTableOption {
	return func(d *DescribeTableDesc) {
		(*Ydb_Table.DescribeTableRequest)(d).SetIncludePartitionStats(true)
	}
}

func WithShardNodesInfo() DescribeTableOption {
	return func(d *DescribeTableDesc) {
		(*Ydb_Table.DescribeTableRequest)(d).SetIncludeShardNodesInfo(true)
	}
}

type (
	CreateTableDesc   Ydb_Table.CreateTableRequest
	CreateTableOption interface {
		ApplyCreateTableOption(d *CreateTableDesc)
	}
)

type (
	profile       Ydb_Table.TableProfile
	ProfileOption interface {
		ApplyProfileOption(p *profile)
	}
)

type (
	storagePolicy      Ydb_Table.StoragePolicy
	compactionPolicy   Ydb_Table.CompactionPolicy
	partitioningPolicy Ydb_Table.PartitioningPolicy
	executionPolicy    Ydb_Table.ExecutionPolicy
	replicationPolicy  Ydb_Table.ReplicationPolicy
	cachingPolicy      Ydb_Table.CachingPolicy
)

type column struct {
	name string
	typ  types.Type
}

func (c column) ApplyAlterTableOption(d *AlterTableDesc) {
	rd := (*Ydb_Table.AlterTableRequest)(d)
	rd.SetAddColumns(append(rd.GetAddColumns(), Ydb_Table.ColumnMeta_builder{
		Name: c.name,
		Type: types.TypeToYDB(c.typ),
	}.Build()))
}

func (c column) ApplyCreateTableOption(d *CreateTableDesc) {
	rc := (*Ydb_Table.CreateTableRequest)(d)
	rc.SetColumns(append(rc.GetColumns(), Ydb_Table.ColumnMeta_builder{
		Name: c.name,
		Type: types.TypeToYDB(c.typ),
	}.Build()))
}

func WithColumn(name string, typ types.Type) CreateTableOption {
	return column{
		name: name,
		typ:  typ,
	}
}

type columnMeta Column

func (c columnMeta) ApplyAlterTableOption(d *AlterTableDesc) {
	rd := (*Ydb_Table.AlterTableRequest)(d)
	rd.SetAddColumns(append(rd.GetAddColumns(), Column(c).toYDB()))
}

func (c columnMeta) ApplyCreateTableOption(d *CreateTableDesc) {
	rc := (*Ydb_Table.CreateTableRequest)(d)
	rc.SetColumns(append(rc.GetColumns(), Column(c).toYDB()))
}

func WithColumnMeta(column Column) CreateTableOption {
	return columnMeta(column)
}

type primaryKeyColumn []string

func (columns primaryKeyColumn) ApplyCreateTableOption(d *CreateTableDesc) {
	rc := (*Ydb_Table.CreateTableRequest)(d)
	rc.SetPrimaryKey(append(rc.GetPrimaryKey(), columns...))
}

func WithPrimaryKeyColumn(columns ...string) CreateTableOption {
	return primaryKeyColumn(columns)
}

type timeToLiveSettings TimeToLiveSettings

func (settings timeToLiveSettings) ApplyCreateTableOption(d *CreateTableDesc) {
	(*Ydb_Table.CreateTableRequest)(d).SetTtlSettings((*TimeToLiveSettings)(&settings).ToYDB())
}

// WithTimeToLiveSettings defines TTL settings in CreateTable request
func WithTimeToLiveSettings(settings TimeToLiveSettings) CreateTableOption {
	return timeToLiveSettings(settings)
}

type attribute struct {
	key   string
	value string
}

func (a attribute) ApplyAlterTableOption(d *AlterTableDesc) {
	rd := (*Ydb_Table.AlterTableRequest)(d)
	attrs := rd.GetAlterAttributes()
	if attrs == nil {
		attrs = make(map[string]string)
	}
	attrs[a.key] = a.value
	rd.SetAlterAttributes(attrs)
}

func (a attribute) ApplyCreateTableOption(d *CreateTableDesc) {
	rc := (*Ydb_Table.CreateTableRequest)(d)
	attrs := rc.GetAttributes()
	if attrs == nil {
		attrs = make(map[string]string)
	}
	attrs[a.key] = a.value
	rc.SetAttributes(attrs)
}

func WithAttribute(key, value string) CreateTableOption {
	return attribute{
		key:   key,
		value: value,
	}
}

type (
	indexDesc   Ydb_Table.TableIndex
	IndexOption interface {
		ApplyIndexOption(d *indexDesc)
	}
)

type index struct {
	name string
	opts []IndexOption
}

func (i index) ApplyAlterTableOption(d *AlterTableDesc) {
	x := Ydb_Table.TableIndex_builder{
		Name: i.name,
	}.Build()
	for _, opt := range i.opts {
		if opt != nil {
			opt.ApplyIndexOption((*indexDesc)(x))
		}
	}
	rd := (*Ydb_Table.AlterTableRequest)(d)
	rd.SetAddIndexes(append(rd.GetAddIndexes(), x))
}

func (i index) ApplyCreateTableOption(d *CreateTableDesc) {
	x := Ydb_Table.TableIndex_builder{
		Name: i.name,
	}.Build()
	for _, opt := range i.opts {
		if opt != nil {
			opt.ApplyIndexOption((*indexDesc)(x))
		}
	}
	rc := (*Ydb_Table.CreateTableRequest)(d)
	rc.SetIndexes(append(rc.GetIndexes(), x))
}

func WithIndex(name string, opts ...IndexOption) CreateTableOption {
	return index{
		name: name,
		opts: opts,
	}
}

func WithAddIndex(name string, opts ...IndexOption) AlterTableOption {
	return index{
		name: name,
		opts: opts,
	}
}

type dropIndex string

func (i dropIndex) ApplyAlterTableOption(d *AlterTableDesc) {
	rd := (*Ydb_Table.AlterTableRequest)(d)
	rd.SetDropIndexes(append(rd.GetDropIndexes(), string(i)))
}

func WithDropIndex(name string) AlterTableOption {
	return dropIndex(name)
}

type indexColumns []string

func (columns indexColumns) ApplyIndexOption(d *indexDesc) {
	td := (*Ydb_Table.TableIndex)(d)
	td.SetIndexColumns(append(td.GetIndexColumns(), columns...))
}

func WithIndexColumns(columns ...string) IndexOption {
	return indexColumns(columns)
}

type dataColumns []string

func (columns dataColumns) ApplyIndexOption(d *indexDesc) {
	td := (*Ydb_Table.TableIndex)(d)
	td.SetDataColumns(append(td.GetDataColumns(), columns...))
}

func WithDataColumns(columns ...string) IndexOption {
	return dataColumns(columns)
}

func WithIndexType(t IndexType) IndexOption {
	return t
}

type columnFamilies []ColumnFamily

func (cf columnFamilies) ApplyAlterTableOption(d *AlterTableDesc) {
	rd := (*Ydb_Table.AlterTableRequest)(d)
	families := make([]*Ydb_Table.ColumnFamily, len(cf))
	for i := range cf {
		families[i] = cf[i].toYDB()
	}
	rd.SetAddColumnFamilies(families)
}

func (cf columnFamilies) ApplyCreateTableOption(d *CreateTableDesc) {
	rc := (*Ydb_Table.CreateTableRequest)(d)
	families := make([]*Ydb_Table.ColumnFamily, len(cf))
	for i := range cf {
		families[i] = cf[i].toYDB()
	}
	rc.SetColumnFamilies(families)
}

func WithColumnFamilies(cf ...ColumnFamily) CreateTableOption {
	return columnFamilies(cf)
}

type readReplicasSettings ReadReplicasSettings

func (rr readReplicasSettings) ApplyAlterTableOption(d *AlterTableDesc) {
	(*Ydb_Table.AlterTableRequest)(d).SetSetReadReplicasSettings(ReadReplicasSettings(rr).ToYDB())
}

func (rr readReplicasSettings) ApplyCreateTableOption(d *CreateTableDesc) {
	(*Ydb_Table.CreateTableRequest)(d).SetReadReplicasSettings(ReadReplicasSettings(rr).ToYDB())
}

func WithReadReplicasSettings(rr ReadReplicasSettings) CreateTableOption {
	return readReplicasSettings(rr)
}

type storageSettings StorageSettings

func (ss storageSettings) ApplyAlterTableOption(d *AlterTableDesc) {
	(*Ydb_Table.AlterTableRequest)(d).SetAlterStorageSettings(StorageSettings(ss).ToYDB())
}

func (ss storageSettings) ApplyCreateTableOption(d *CreateTableDesc) {
	(*Ydb_Table.CreateTableRequest)(d).SetStorageSettings(StorageSettings(ss).ToYDB())
}

func WithStorageSettings(ss StorageSettings) CreateTableOption {
	return storageSettings(ss)
}

type keyBloomFilter FeatureFlag

func (f keyBloomFilter) ApplyAlterTableOption(d *AlterTableDesc) {
	(*Ydb_Table.AlterTableRequest)(d).SetSetKeyBloomFilter(FeatureFlag(f).ToYDB())
}

func (f keyBloomFilter) ApplyCreateTableOption(d *CreateTableDesc) {
	(*Ydb_Table.CreateTableRequest)(d).SetKeyBloomFilter(FeatureFlag(f).ToYDB())
}

func WithKeyBloomFilter(f FeatureFlag) CreateTableOption {
	return keyBloomFilter(f)
}

func WithPartitions(p Partitions) CreateTableOption {
	return p
}

type uniformPartitions uint64

func (u uniformPartitions) ApplyCreateTableOption(d *CreateTableDesc) {
	(*Ydb_Table.CreateTableRequest)(d).SetUniformPartitions(uint64(u))
}

func (u uniformPartitions) isPartitions() {}

func WithUniformPartitions(n uint64) Partitions {
	return uniformPartitions(n)
}

type explicitPartitions []value.Value

func (e explicitPartitions) ApplyCreateTableOption(d *CreateTableDesc) {
	values := make([]*Ydb.TypedValue, len(e))
	for i := range values {
		values[i] = value.ToYDB(e[i])
	}
	(*Ydb_Table.CreateTableRequest)(d).SetPartitionAtKeys(Ydb_Table.ExplicitPartitions_builder{
		SplitPoints: values,
	}.Build())
}

func (e explicitPartitions) isPartitions() {}

func WithExplicitPartitions(splitPoints ...value.Value) Partitions {
	return explicitPartitions(splitPoints)
}

type profileOption []ProfileOption

func (opts profileOption) ApplyCreateTableOption(d *CreateTableDesc) {
	rc := (*Ydb_Table.CreateTableRequest)(d)
	if rc.GetProfile() == nil {
		rc.SetProfile(&Ydb_Table.TableProfile{})
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyProfileOption((*profile)(rc.GetProfile()))
		}
	}
}

func WithProfile(opts ...ProfileOption) CreateTableOption {
	return profileOption(opts)
}

type profilePresetProfileOption string

func (preset profilePresetProfileOption) ApplyProfileOption(p *profile) {
	(*Ydb_Table.TableProfile)(p).SetPresetName(string(preset))
}

func WithProfilePreset(name string) ProfileOption {
	return profilePresetProfileOption(name)
}

type storagePolicyProfileOption []StoragePolicyOption

func (opts storagePolicyProfileOption) ApplyProfileOption(p *profile) {
	pp := (*Ydb_Table.TableProfile)(p)
	if pp.GetStoragePolicy() == nil {
		pp.SetStoragePolicy(&Ydb_Table.StoragePolicy{})
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*storagePolicy)(pp.GetStoragePolicy()))
		}
	}
}

func WithStoragePolicy(opts ...StoragePolicyOption) ProfileOption {
	return storagePolicyProfileOption(opts)
}

type compactionPolicyProfileOption []CompactionPolicyOption

func (opts compactionPolicyProfileOption) ApplyProfileOption(p *profile) {
	pp := (*Ydb_Table.TableProfile)(p)
	if pp.GetCompactionPolicy() == nil {
		pp.SetCompactionPolicy(&Ydb_Table.CompactionPolicy{})
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*compactionPolicy)(pp.GetCompactionPolicy()))
		}
	}
}

func WithCompactionPolicy(opts ...CompactionPolicyOption) ProfileOption {
	return compactionPolicyProfileOption(opts)
}

type partitioningPolicyProfileOption []PartitioningPolicyOption

func (opts partitioningPolicyProfileOption) ApplyProfileOption(p *profile) {
	pp := (*Ydb_Table.TableProfile)(p)
	if pp.GetPartitioningPolicy() == nil {
		pp.SetPartitioningPolicy(&Ydb_Table.PartitioningPolicy{})
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*partitioningPolicy)(pp.GetPartitioningPolicy()))
		}
	}
}

func WithPartitioningPolicy(opts ...PartitioningPolicyOption) ProfileOption {
	return partitioningPolicyProfileOption(opts)
}

type executionPolicyProfileOption []ExecutionPolicyOption

func (opts executionPolicyProfileOption) ApplyProfileOption(p *profile) {
	pp := (*Ydb_Table.TableProfile)(p)
	if pp.GetExecutionPolicy() == nil {
		pp.SetExecutionPolicy(&Ydb_Table.ExecutionPolicy{})
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*executionPolicy)(pp.GetExecutionPolicy()))
		}
	}
}

func WithExecutionPolicy(opts ...ExecutionPolicyOption) ProfileOption {
	return executionPolicyProfileOption(opts)
}

type replicationPolicyProfileOption []ReplicationPolicyOption

func (opts replicationPolicyProfileOption) ApplyProfileOption(p *profile) {
	pp := (*Ydb_Table.TableProfile)(p)
	if pp.GetReplicationPolicy() == nil {
		pp.SetReplicationPolicy(&Ydb_Table.ReplicationPolicy{})
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*replicationPolicy)(pp.GetReplicationPolicy()))
		}
	}
}

func WithReplicationPolicy(opts ...ReplicationPolicyOption) ProfileOption {
	return replicationPolicyProfileOption(opts)
}

type cachingPolicyProfileOption []CachingPolicyOption

func (opts cachingPolicyProfileOption) ApplyProfileOption(p *profile) {
	pp := (*Ydb_Table.TableProfile)(p)
	if pp.GetCachingPolicy() == nil {
		pp.SetCachingPolicy(&Ydb_Table.CachingPolicy{})
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*cachingPolicy)(pp.GetCachingPolicy()))
		}
	}
}

func WithCachingPolicy(opts ...CachingPolicyOption) ProfileOption {
	return cachingPolicyProfileOption(opts)
}

type (
	StoragePolicyOption      func(*storagePolicy)
	CompactionPolicyOption   func(*compactionPolicy)
	PartitioningPolicyOption func(*partitioningPolicy)
	ExecutionPolicyOption    func(*executionPolicy)
	ReplicationPolicyOption  func(*replicationPolicy)
	CachingPolicyOption      func(*cachingPolicy)
)

func WithStoragePolicyPreset(name string) StoragePolicyOption {
	return func(s *storagePolicy) {
		(*Ydb_Table.StoragePolicy)(s).SetPresetName(name)
	}
}

func WithStoragePolicySyslog(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		(*Ydb_Table.StoragePolicy)(s).SetSyslog(Ydb_Table.StoragePool_builder{Media: kind}.Build())
	}
}

func WithStoragePolicyLog(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		(*Ydb_Table.StoragePolicy)(s).SetLog(Ydb_Table.StoragePool_builder{Media: kind}.Build())
	}
}

func WithStoragePolicyData(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		(*Ydb_Table.StoragePolicy)(s).SetData(Ydb_Table.StoragePool_builder{Media: kind}.Build())
	}
}

func WithStoragePolicyExternal(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		(*Ydb_Table.StoragePolicy)(s).SetExternal(Ydb_Table.StoragePool_builder{Media: kind}.Build())
	}
}

func WithStoragePolicyKeepInMemory(flag FeatureFlag) StoragePolicyOption {
	return func(s *storagePolicy) {
		(*Ydb_Table.StoragePolicy)(s).SetKeepInMemory(flag.ToYDB())
	}
}

func WithCompactionPolicyPreset(name string) CompactionPolicyOption {
	return func(c *compactionPolicy) {
		(*Ydb_Table.CompactionPolicy)(c).SetPresetName(name)
	}
}

func WithPartitioningPolicyPreset(name string) PartitioningPolicyOption {
	return func(p *partitioningPolicy) {
		(*Ydb_Table.PartitioningPolicy)(p).SetPresetName(name)
	}
}

func WithPartitioningPolicyMode(mode PartitioningMode) PartitioningPolicyOption {
	return func(p *partitioningPolicy) {
		(*Ydb_Table.PartitioningPolicy)(p).SetAutoPartitioning(mode.toYDB())
	}
}

// Deprecated: use WithUniformPartitions instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithPartitioningPolicyUniformPartitions(n uint64) PartitioningPolicyOption {
	return func(p *partitioningPolicy) {
		(*Ydb_Table.PartitioningPolicy)(p).SetUniformPartitions(n)
	}
}

// Deprecated: use WithExplicitPartitions instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithPartitioningPolicyExplicitPartitions(splitPoints ...value.Value) PartitioningPolicyOption {
	return func(p *partitioningPolicy) {
		values := make([]*Ydb.TypedValue, len(splitPoints))
		for i := range values {
			values[i] = value.ToYDB(splitPoints[i])
		}
		(*Ydb_Table.PartitioningPolicy)(p).SetExplicitPartitions(Ydb_Table.ExplicitPartitions_builder{
			SplitPoints: values,
		}.Build())
	}
}

func WithReplicationPolicyPreset(name string) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		(*Ydb_Table.ReplicationPolicy)(e).SetPresetName(name)
	}
}

func WithReplicationPolicyReplicasCount(n uint32) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		(*Ydb_Table.ReplicationPolicy)(e).SetReplicasCount(n)
	}
}

func WithReplicationPolicyCreatePerAZ(flag FeatureFlag) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		(*Ydb_Table.ReplicationPolicy)(e).SetCreatePerAvailabilityZone(flag.ToYDB())
	}
}

func WithReplicationPolicyAllowPromotion(flag FeatureFlag) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		(*Ydb_Table.ReplicationPolicy)(e).SetAllowPromotion(flag.ToYDB())
	}
}

func WithExecutionPolicyPreset(name string) ExecutionPolicyOption {
	return func(e *executionPolicy) {
		(*Ydb_Table.ExecutionPolicy)(e).SetPresetName(name)
	}
}

func WithCachingPolicyPreset(name string) CachingPolicyOption {
	return func(e *cachingPolicy) {
		(*Ydb_Table.CachingPolicy)(e).SetPresetName(name)
	}
}

type partitioningSettingsObject PartitioningSettings

func (ps partitioningSettingsObject) ApplyAlterTableOption(d *AlterTableDesc) {
	(*Ydb_Table.AlterTableRequest)(d).SetAlterPartitioningSettings(PartitioningSettings(ps).toYDB())
}

func (ps partitioningSettingsObject) ApplyCreateTableOption(d *CreateTableDesc) {
	(*Ydb_Table.CreateTableRequest)(d).SetPartitioningSettings(PartitioningSettings(ps).toYDB())
}

func WithPartitioningSettingsObject(ps PartitioningSettings) CreateTableOption {
	return partitioningSettingsObject(ps)
}

type partitioningSettings []PartitioningSettingsOption

func (opts partitioningSettings) ApplyCreateTableOption(d *CreateTableDesc) {
	settings := &ydbPartitioningSettings{}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyPartitioningSettingsOption(settings)
		}
	}
	(*Ydb_Table.CreateTableRequest)(d).SetPartitioningSettings((*Ydb_Table.PartitioningSettings)(settings))
}

func WithPartitioningSettings(opts ...PartitioningSettingsOption) CreateTableOption {
	return partitioningSettings(opts)
}

type (
	ydbPartitioningSettings    Ydb_Table.PartitioningSettings
	PartitioningSettingsOption interface {
		ApplyPartitioningSettingsOption(settings *ydbPartitioningSettings)
	}
)

type partitioningBySizePartitioningSettingsOption FeatureFlag

func (flag partitioningBySizePartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	(*Ydb_Table.PartitioningSettings)(settings).SetPartitioningBySize(FeatureFlag(flag).ToYDB())
}

func WithPartitioningBySize(flag FeatureFlag) PartitioningSettingsOption {
	return partitioningBySizePartitioningSettingsOption(flag)
}

type partitionSizeMbPartitioningSettingsOption uint64

func (partitionSizeMb partitionSizeMbPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	(*Ydb_Table.PartitioningSettings)(settings).SetPartitionSizeMb(uint64(partitionSizeMb))
}

func WithPartitionSizeMb(partitionSizeMb uint64) PartitioningSettingsOption {
	return partitionSizeMbPartitioningSettingsOption(partitionSizeMb)
}

type partitioningByLoadPartitioningSettingsOption FeatureFlag

func (flag partitioningByLoadPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	(*Ydb_Table.PartitioningSettings)(settings).SetPartitioningByLoad(FeatureFlag(flag).ToYDB())
}

func WithPartitioningByLoad(flag FeatureFlag) PartitioningSettingsOption {
	return partitioningByLoadPartitioningSettingsOption(flag)
}

type partitioningByPartitioningSettingsOption []string

func (columns partitioningByPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	(*Ydb_Table.PartitioningSettings)(settings).SetPartitionBy(columns)
}

func WithPartitioningBy(columns []string) PartitioningSettingsOption {
	return partitioningByPartitioningSettingsOption(columns)
}

type minPartitionsCountPartitioningSettingsOption uint64

func (minPartitionsCount minPartitionsCountPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	(*Ydb_Table.PartitioningSettings)(settings).SetMinPartitionsCount(uint64(minPartitionsCount))
}

func WithMinPartitionsCount(minPartitionsCount uint64) PartitioningSettingsOption {
	return minPartitionsCountPartitioningSettingsOption(minPartitionsCount)
}

type maxPartitionsCountPartitioningSettingsOption uint64

func (maxPartitionsCount maxPartitionsCountPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	(*Ydb_Table.PartitioningSettings)(settings).SetMaxPartitionsCount(uint64(maxPartitionsCount))
}

func WithMaxPartitionsCount(maxPartitionsCount uint64) PartitioningSettingsOption {
	return maxPartitionsCountPartitioningSettingsOption(maxPartitionsCount)
}

type (
	DropTableDesc   Ydb_Table.DropTableRequest
	DropTableOption interface {
		ApplyDropTableOption(desc *DropTableDesc)
	}
)

type (
	AlterTableDesc   Ydb_Table.AlterTableRequest
	AlterTableOption interface {
		ApplyAlterTableOption(desc *AlterTableDesc)
	}
)

// WithAddColumn adds column in AlterTable request
func WithAddColumn(name string, typ types.Type) AlterTableOption {
	return column{
		name: name,
		typ:  typ,
	}
}

// WithAlterAttribute changes attribute in AlterTable request
func WithAlterAttribute(key, value string) AlterTableOption {
	return attribute{
		key:   key,
		value: value,
	}
}

// WithAddAttribute adds attribute to table in AlterTable request
func WithAddAttribute(key, value string) AlterTableOption {
	return attribute{
		key:   key,
		value: value,
	}
}

// WithDropAttribute drops attribute from table in AlterTable request
func WithDropAttribute(key string) AlterTableOption {
	return attribute{
		key: key,
	}
}

func WithAddColumnMeta(column Column) AlterTableOption {
	return columnMeta(column)
}

type dropColumn string

func (name dropColumn) ApplyAlterTableOption(d *AlterTableDesc) {
	rd := (*Ydb_Table.AlterTableRequest)(d)
	rd.SetDropColumns(append(rd.GetDropColumns(), string(name)))
}

func WithDropColumn(name string) AlterTableOption {
	return dropColumn(name)
}

func WithAddColumnFamilies(cf ...ColumnFamily) AlterTableOption {
	return columnFamilies(cf)
}

func WithAlterColumnFamilies(cf ...ColumnFamily) AlterTableOption {
	return columnFamilies(cf)
}

func WithAlterReadReplicasSettings(rr ReadReplicasSettings) AlterTableOption {
	return readReplicasSettings(rr)
}

func WithAlterStorageSettings(ss StorageSettings) AlterTableOption {
	return storageSettings(ss)
}

func WithAlterKeyBloomFilter(f FeatureFlag) AlterTableOption {
	return keyBloomFilter(f)
}

func WithAlterPartitionSettingsObject(ps PartitioningSettings) AlterTableOption {
	return partitioningSettingsObject(ps)
}

func (settings timeToLiveSettings) ApplyAlterTableOption(d *AlterTableDesc) {
	(*Ydb_Table.AlterTableRequest)(d).SetSetTtlSettings((*TimeToLiveSettings)(&settings).ToYDB())
}

// WithSetTimeToLiveSettings appends TTL settings in AlterTable request
func WithSetTimeToLiveSettings(settings TimeToLiveSettings) AlterTableOption {
	return timeToLiveSettings(settings)
}

type dropTimeToLive struct{}

func (dropTimeToLive) ApplyAlterTableOption(d *AlterTableDesc) {
	(*Ydb_Table.AlterTableRequest)(d).SetDropTtlSettings(&emptypb.Empty{})
}

// WithDropTimeToLive drops TTL settings in AlterTable request
func WithDropTimeToLive() AlterTableOption {
	return dropTimeToLive{}
}

type (
	CopyTableDesc   Ydb_Table.CopyTableRequest
	CopyTableOption func(*CopyTableDesc)
)

type (
	CopyTablesDesc   Ydb_Table.CopyTablesRequest
	CopyTablesOption func(*CopyTablesDesc)
)

func CopyTablesItem(src, dst string, omitIndexes bool) CopyTablesOption {
	return func(desc *CopyTablesDesc) {
		rd := (*Ydb_Table.CopyTablesRequest)(desc)
		rd.SetTables(append(rd.GetTables(), Ydb_Table.CopyTableItem_builder{
			SourcePath:      src,
			DestinationPath: dst,
			OmitIndexes:     omitIndexes,
		}.Build()))
	}
}

type (
	RenameTablesDesc   Ydb_Table.RenameTablesRequest
	RenameTablesOption func(desc *RenameTablesDesc)
)

func RenameTablesItem(src, dst string, replaceDestination bool) RenameTablesOption {
	return func(desc *RenameTablesDesc) {
		rd := (*Ydb_Table.RenameTablesRequest)(desc)
		rd.SetTables(append(rd.GetTables(), Ydb_Table.RenameTableItem_builder{
			SourcePath:         src,
			DestinationPath:    dst,
			ReplaceDestination: replaceDestination,
		}.Build()))
	}
}

type (
	ExecuteSchemeQueryDesc   Ydb_Table.ExecuteSchemeQueryRequest
	ExecuteSchemeQueryOption func(*ExecuteSchemeQueryDesc)
)

type (
	ExecuteDataQueryDesc struct {
		*Ydb_Table.ExecuteDataQueryRequest

		IgnoreTruncated bool
	}
	ExecuteDataQueryOption interface {
		ApplyExecuteDataQueryOption(d *ExecuteDataQueryDesc) []grpc.CallOption
	}
	executeDataQueryOptionFunc func(d *ExecuteDataQueryDesc) []grpc.CallOption
)

func (f executeDataQueryOptionFunc) ApplyExecuteDataQueryOption(d *ExecuteDataQueryDesc) []grpc.CallOption {
	return f(d)
}

var _ ExecuteDataQueryOption = executeDataQueryOptionFunc(nil)

// Deprecated: Use ExecuteDataQueryRequest.ProtoReflect().Descriptor() instead.
func (d ExecuteDataQueryDesc) Descriptor() ([]byte, []int) {
	md := (&Ydb_Table.ExecuteDataQueryRequest{}).ProtoReflect().Descriptor()
	idxPath := []int{md.Index()}

	rawDesc, err := proto.Marshal(protodesc.ToFileDescriptorProto(md.ParentFile()))
	if err != nil {
		return nil, idxPath
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err = zw.Write(rawDesc); err != nil {
		_ = zw.Close()

		return nil, idxPath
	}
	if err = zw.Close(); err != nil {
		return nil, idxPath
	}

	return buf.Bytes(), idxPath
}

type (
	CommitTransactionDesc   Ydb_Table.CommitTransactionRequest
	CommitTransactionOption func(*CommitTransactionDesc)
)

type (
	queryCachePolicy       Ydb_Table.QueryCachePolicy
	QueryCachePolicyOption func(*queryCachePolicy)
)

// WithKeepInCache manages keep-in-cache flag in query cache policy
//
// By default all data queries executes with keep-in-cache policy
func WithKeepInCache(keepInCache bool) ExecuteDataQueryOption {
	return withQueryCachePolicy(
		withQueryCachePolicyKeepInCache(keepInCache),
	)
}

type withCallOptions []grpc.CallOption

func (opts withCallOptions) ApplyExecuteScanQueryOption(d *ExecuteScanQueryDesc) []grpc.CallOption {
	return opts
}

func (opts withCallOptions) ApplyBulkUpsertOption() []grpc.CallOption {
	return opts
}

func (opts withCallOptions) ApplyExecuteDataQueryOption(
	d *ExecuteDataQueryDesc,
) []grpc.CallOption {
	return opts
}

// WithCallOptions appends flag of commit transaction with executing query
func WithCallOptions(opts ...grpc.CallOption) withCallOptions {
	return opts
}

// WithCommit appends flag of commit transaction with executing query
func WithCommit() ExecuteDataQueryOption {
	return executeDataQueryOptionFunc(func(desc *ExecuteDataQueryDesc) []grpc.CallOption {
		desc.GetTxControl().SetCommitTx(true)

		return nil
	})
}

// WithIgnoreTruncated mark truncated result as good (without error)
func WithIgnoreTruncated() ExecuteDataQueryOption {
	return executeDataQueryOptionFunc(func(desc *ExecuteDataQueryDesc) []grpc.CallOption {
		desc.IgnoreTruncated = true

		return nil
	})
}

// WithQueryCachePolicyKeepInCache manages keep-in-cache policy
//
// Deprecated: data queries always executes with enabled keep-in-cache policy.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithQueryCachePolicyKeepInCache() QueryCachePolicyOption {
	return withQueryCachePolicyKeepInCache(true)
}

func withQueryCachePolicyKeepInCache(keepInCache bool) QueryCachePolicyOption {
	return func(p *queryCachePolicy) {
		(*Ydb_Table.QueryCachePolicy)(p).SetKeepInCache(keepInCache)
	}
}

// WithQueryCachePolicy manages query cache policy
//
// Deprecated: use WithKeepInCache for disabling keep-in-cache policy.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithQueryCachePolicy(opts ...QueryCachePolicyOption) ExecuteDataQueryOption {
	return withQueryCachePolicy(opts...)
}

func withQueryCachePolicy(opts ...QueryCachePolicyOption) ExecuteDataQueryOption {
	return executeDataQueryOptionFunc(func(d *ExecuteDataQueryDesc) []grpc.CallOption {
		if d.GetQueryCachePolicy() == nil {
			d.SetQueryCachePolicy(Ydb_Table.QueryCachePolicy_builder{
				KeepInCache: true,
			}.Build())
		}
		for _, opt := range opts {
			if opt != nil {
				opt((*queryCachePolicy)(d.GetQueryCachePolicy()))
			}
		}

		return nil
	})
}

func WithCommitCollectStatsModeNone() CommitTransactionOption {
	return func(d *CommitTransactionDesc) {
		(*Ydb_Table.CommitTransactionRequest)(d).SetCollectStats(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_NONE)
	}
}

func WithCommitCollectStatsModeBasic() CommitTransactionOption {
	return func(d *CommitTransactionDesc) {
		(*Ydb_Table.CommitTransactionRequest)(d).SetCollectStats(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_BASIC)
	}
}

func WithCollectStatsModeNone() ExecuteDataQueryOption {
	return executeDataQueryOptionFunc(func(d *ExecuteDataQueryDesc) []grpc.CallOption {
		d.ExecuteDataQueryRequest.SetCollectStats(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_NONE)

		return nil
	})
}

func WithCollectStatsModeBasic() ExecuteDataQueryOption {
	return executeDataQueryOptionFunc(func(d *ExecuteDataQueryDesc) []grpc.CallOption {
		d.ExecuteDataQueryRequest.SetCollectStats(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_BASIC)

		return nil
	})
}

func WithCollectStatsMode(mode Ydb_Table.QueryStatsCollection_Mode) ExecuteDataQueryOption {
	return executeDataQueryOptionFunc(func(d *ExecuteDataQueryDesc) []grpc.CallOption {
		d.ExecuteDataQueryRequest.SetCollectStats(mode)

		return nil
	})
}

type (
	BulkUpsertOption interface {
		ApplyBulkUpsertOption() []grpc.CallOption
	}
)

type (
	ExecuteScanQueryDesc   Ydb_Table.ExecuteScanQueryRequest
	ExecuteScanQueryOption interface {
		ApplyExecuteScanQueryOption(d *ExecuteScanQueryDesc) []grpc.CallOption
	}
	executeScanQueryOptionFunc func(*ExecuteScanQueryDesc) []grpc.CallOption
)

func (f executeScanQueryOptionFunc) ApplyExecuteScanQueryOption(d *ExecuteScanQueryDesc) []grpc.CallOption {
	return f(d)
}

var _ ExecuteScanQueryOption = executeScanQueryOptionFunc(nil)

// WithExecuteScanQueryMode defines scan query mode: execute or explain
func WithExecuteScanQueryMode(m ExecuteScanQueryRequestMode) ExecuteScanQueryOption {
	return executeScanQueryOptionFunc(func(desc *ExecuteScanQueryDesc) []grpc.CallOption {
		(*Ydb_Table.ExecuteScanQueryRequest)(desc).SetMode(m.toYDB())

		return nil
	})
}

// ExecuteScanQueryStatsType specified scan query mode
type ExecuteScanQueryStatsType uint32

const (
	ExecuteScanQueryStatsTypeNone = iota
	ExecuteScanQueryStatsTypeBasic
	ExecuteScanQueryStatsTypeFull
)

func (stats ExecuteScanQueryStatsType) toYDB() Ydb_Table.QueryStatsCollection_Mode {
	switch stats {
	case ExecuteScanQueryStatsTypeNone:
		return Ydb_Table.QueryStatsCollection_STATS_COLLECTION_NONE
	case ExecuteScanQueryStatsTypeBasic:
		return Ydb_Table.QueryStatsCollection_STATS_COLLECTION_BASIC
	case ExecuteScanQueryStatsTypeFull:
		return Ydb_Table.QueryStatsCollection_STATS_COLLECTION_FULL
	default:
		return Ydb_Table.QueryStatsCollection_STATS_COLLECTION_UNSPECIFIED
	}
}

// WithExecuteScanQueryStats defines query statistics mode
func WithExecuteScanQueryStats(stats ExecuteScanQueryStatsType) ExecuteScanQueryOption {
	return executeScanQueryOptionFunc(func(desc *ExecuteScanQueryDesc) []grpc.CallOption {
		(*Ydb_Table.ExecuteScanQueryRequest)(desc).SetCollectStats(stats.toYDB())

		return nil
	})
}

var (
	_ ReadRowsOption  = readColumnsOption{}
	_ ReadTableOption = readOrderedOption{}
	_ ReadTableOption = readKeyRangeOption{}
	_ ReadTableOption = readGreaterOrEqualOption{}
	_ ReadTableOption = readLessOrEqualOption{}
	_ ReadTableOption = readLessOption{}
	_ ReadTableOption = readGreaterOption{}
	_ ReadTableOption = readRowLimitOption(0)
)

type (
	ReadRowsDesc   Ydb_Table.ReadRowsRequest
	ReadRowsOption interface {
		ApplyReadRowsOption(desc *ReadRowsDesc)
	}

	ReadTableDesc   Ydb_Table.ReadTableRequest
	ReadTableOption interface {
		ApplyReadTableOption(desc *ReadTableDesc)
	}

	readColumnsOption        []string
	readOrderedOption        struct{}
	readSnapshotOption       bool
	readKeyRangeOption       KeyRange
	readGreaterOrEqualOption struct{ value.Value }
	readLessOrEqualOption    struct{ value.Value }
	readLessOption           struct{ value.Value }
	readGreaterOption        struct{ value.Value }
	readRowLimitOption       uint64
)

func (n readRowLimitOption) ApplyReadTableOption(desc *ReadTableDesc) {
	(*Ydb_Table.ReadTableRequest)(desc).SetRowLimit(uint64(n))
}

func (x readGreaterOption) ApplyReadTableOption(desc *ReadTableDesc) {
	desc.initKeyRange()
	(*Ydb_Table.ReadTableRequest)(desc).GetKeyRange().SetGreater(proto.ValueOrDefault(value.ToYDB(x)))
}

func (x readLessOrEqualOption) ApplyReadTableOption(desc *ReadTableDesc) {
	desc.initKeyRange()
	(*Ydb_Table.ReadTableRequest)(desc).GetKeyRange().SetLessOrEqual(proto.ValueOrDefault(value.ToYDB(x)))
}

func (x readLessOption) ApplyReadTableOption(desc *ReadTableDesc) {
	desc.initKeyRange()
	(*Ydb_Table.ReadTableRequest)(desc).GetKeyRange().SetLess(proto.ValueOrDefault(value.ToYDB(x)))
}

func (columns readColumnsOption) ApplyReadRowsOption(desc *ReadRowsDesc) {
	rd := (*Ydb_Table.ReadRowsRequest)(desc)
	rd.SetColumns(append(rd.GetColumns(), columns...))
}

func (columns readColumnsOption) ApplyReadTableOption(desc *ReadTableDesc) {
	rd := (*Ydb_Table.ReadTableRequest)(desc)
	rd.SetColumns(append(rd.GetColumns(), columns...))
}

func (readOrderedOption) ApplyReadTableOption(desc *ReadTableDesc) {
	(*Ydb_Table.ReadTableRequest)(desc).SetOrdered(true)
}

func (b readSnapshotOption) ApplyReadTableOption(desc *ReadTableDesc) {
	if b {
		(*Ydb_Table.ReadTableRequest)(desc).SetUseSnapshot(FeatureEnabled.ToYDB())
	} else {
		(*Ydb_Table.ReadTableRequest)(desc).SetUseSnapshot(FeatureDisabled.ToYDB())
	}
}

func (x readKeyRangeOption) ApplyReadTableOption(desc *ReadTableDesc) {
	desc.initKeyRange()
	kr := (*Ydb_Table.ReadTableRequest)(desc).GetKeyRange()
	if x.From != nil {
		kr.SetGreaterOrEqual(proto.ValueOrDefault(value.ToYDB(x.From)))
	}
	if x.To != nil {
		kr.SetLess(proto.ValueOrDefault(value.ToYDB(x.To)))
	}
}

func (x readGreaterOrEqualOption) ApplyReadTableOption(desc *ReadTableDesc) {
	desc.initKeyRange()
	(*Ydb_Table.ReadTableRequest)(desc).GetKeyRange().SetGreaterOrEqual(proto.ValueOrDefault(value.ToYDB(x)))
}

func ReadColumn(name string) readColumnsOption {
	return []string{name}
}

func ReadColumns(names ...string) readColumnsOption {
	return names
}

func ReadOrdered() ReadTableOption {
	return readOrderedOption{}
}

func ReadFromSnapshot(b bool) ReadTableOption {
	return readSnapshotOption(b)
}

// ReadKeyRange returns ReadTableOption which makes ReadTable read values
// in range [x.From, x.To).
//
// Both x.From and x.To may be nil.
func ReadKeyRange(x KeyRange) ReadTableOption {
	return readKeyRangeOption(x)
}

func ReadGreater(x value.Value) ReadTableOption {
	return readGreaterOption{x}
}

func ReadGreaterOrEqual(x value.Value) ReadTableOption {
	return readGreaterOrEqualOption{x}
}

func ReadLess(x value.Value) ReadTableOption {
	return readLessOption{x}
}

func ReadLessOrEqual(x value.Value) ReadTableOption {
	return readLessOrEqualOption{x}
}

func ReadRowLimit(n uint64) ReadTableOption {
	return readRowLimitOption(n)
}

func (d *ReadTableDesc) initKeyRange() {
	rd := (*Ydb_Table.ReadTableRequest)(d)
	if rd.GetKeyRange() == nil {
		rd.SetKeyRange(&Ydb_Table.KeyRange{})
	}
}
