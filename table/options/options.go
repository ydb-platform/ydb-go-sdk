package options

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func WithShardKeyBounds() DescribeTableOption {
	return func(d *DescribeTableDesc) {
		d.IncludeShardKeyBounds = true
	}
}

func WithTableStats() DescribeTableOption {
	return func(d *DescribeTableDesc) {
		d.IncludeTableStats = true
	}
}

func WithPartitionStats() DescribeTableOption {
	return func(d *DescribeTableDesc) {
		d.IncludePartitionStats = true
	}
}

type (
	CreateTableDesc   Ydb_Table.CreateTableRequest
	CreateTableOption interface {
		ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator)
	}
)

type (
	profile       Ydb_Table.TableProfile
	ProfileOption interface {
		ApplyProfileOption(p *profile, a *allocator.Allocator)
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

func (c column) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.AddColumns = append(d.AddColumns, &Ydb_Table.ColumnMeta{
		Name: c.name,
		Type: value.TypeToYDB(c.typ, a),
	})
}

func (c column) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.Columns = append(d.Columns, &Ydb_Table.ColumnMeta{
		Name: c.name,
		Type: value.TypeToYDB(c.typ, a),
	})
}

func WithColumn(name string, typ types.Type) CreateTableOption {
	return column{
		name: name,
		typ:  typ,
	}
}

type columnMeta Column

func (c columnMeta) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.AddColumns = append(d.AddColumns, Column(c).toYDB(a))
}

func (c columnMeta) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.Columns = append(d.Columns, Column(c).toYDB(a))
}

func WithColumnMeta(column Column) CreateTableOption {
	return columnMeta(column)
}

type primaryKeyColumn []string

func (columns primaryKeyColumn) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.PrimaryKey = append(d.PrimaryKey, columns...)
}

func WithPrimaryKeyColumn(columns ...string) CreateTableOption {
	return primaryKeyColumn(columns)
}

type timeToLiveSettings TimeToLiveSettings

func (settings timeToLiveSettings) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.TtlAction = &Ydb_Table.AlterTableRequest_SetTtlSettings{
		SetTtlSettings: (*TimeToLiveSettings)(&settings).ToYDB(),
	}
}

func (settings timeToLiveSettings) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.TtlSettings = (*TimeToLiveSettings)(&settings).ToYDB()
}

func WithTimeToLiveSettings(settings TimeToLiveSettings) CreateTableOption {
	return timeToLiveSettings(settings)
}

type attribute struct {
	key   string
	value string
}

func (a attribute) ApplyAlterTableOption(d *AlterTableDesc, _ *allocator.Allocator) {
	if d.AlterAttributes == nil {
		d.AlterAttributes = make(map[string]string)
	}
	d.AlterAttributes[a.key] = a.value
}

func (a attribute) ApplyCreateTableOption(d *CreateTableDesc, _ *allocator.Allocator) {
	if d.Attributes == nil {
		d.Attributes = make(map[string]string)
	}
	d.Attributes[a.key] = a.value
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

func (i index) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	x := &Ydb_Table.TableIndex{
		Name: i.name,
	}
	for _, opt := range i.opts {
		opt.ApplyIndexOption((*indexDesc)(x))
	}
	d.Indexes = append(d.Indexes, x)
}

func WithIndex(name string, opts ...IndexOption) CreateTableOption {
	return index{
		name: name,
		opts: opts,
	}
}

type indexColumns []string

func (columns indexColumns) ApplyIndexOption(d *indexDesc) {
	d.IndexColumns = append(d.IndexColumns, columns...)
}

func WithIndexColumns(columns ...string) IndexOption {
	return indexColumns(columns)
}

type dataColumns []string

func (columns dataColumns) ApplyIndexOption(d *indexDesc) {
	d.DataColumns = append(d.DataColumns, columns...)
}

func WithDataColumns(columns ...string) IndexOption {
	return dataColumns(columns)
}

type indexType struct {
	t IndexType
}

func (i indexType) ApplyIndexOption(d *indexDesc) {
	i.t.setup(d)
}

func WithIndexType(t IndexType) IndexOption {
	return indexType{t: t}
}

type columnFamilies []ColumnFamily

func (cf columnFamilies) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.AddColumnFamilies = make([]*Ydb_Table.ColumnFamily, len(cf))
	for i, c := range cf {
		d.AddColumnFamilies[i] = c.toYDB()
	}
}

func (cf columnFamilies) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.ColumnFamilies = make([]*Ydb_Table.ColumnFamily, len(cf))
	for i, c := range cf {
		d.ColumnFamilies[i] = c.toYDB()
	}
}

func WithColumnFamilies(cf ...ColumnFamily) CreateTableOption {
	return columnFamilies(cf)
}

type readReplicasSettings ReadReplicasSettings

func (rr readReplicasSettings) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.SetReadReplicasSettings = ReadReplicasSettings(rr).ToYDB()
}

func (rr readReplicasSettings) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.ReadReplicasSettings = ReadReplicasSettings(rr).ToYDB()
}

func WithReadReplicasSettings(rr ReadReplicasSettings) CreateTableOption {
	return readReplicasSettings(rr)
}

type storageSettings StorageSettings

func (ss storageSettings) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.AlterStorageSettings = StorageSettings(ss).ToYDB()
}

func (ss storageSettings) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.StorageSettings = StorageSettings(ss).ToYDB()
}

func WithStorageSettings(ss StorageSettings) CreateTableOption {
	return storageSettings(ss)
}

type keyBloomFilter FeatureFlag

func (f keyBloomFilter) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.SetKeyBloomFilter = FeatureFlag(f).ToYDB()
}

func (f keyBloomFilter) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.KeyBloomFilter = FeatureFlag(f).ToYDB()
}

func WithKeyBloomFilter(f FeatureFlag) CreateTableOption {
	return keyBloomFilter(f)
}

type profileOption []ProfileOption

func (opts profileOption) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	if d.Profile == nil {
		d.Profile = new(Ydb_Table.TableProfile)
	}
	for _, opt := range opts {
		opt.ApplyProfileOption((*profile)(d.Profile), a)
	}
}

func WithProfile(opts ...ProfileOption) CreateTableOption {
	return profileOption(opts)
}

type profilePresetProfileOption string

func (preset profilePresetProfileOption) ApplyProfileOption(p *profile, a *allocator.Allocator) {
	p.PresetName = string(preset)
}

func WithProfilePreset(name string) ProfileOption {
	return profilePresetProfileOption(name)
}

type storagePolicyProfileOption []StoragePolicyOption

func (opts storagePolicyProfileOption) ApplyProfileOption(p *profile, a *allocator.Allocator) {
	if p.StoragePolicy == nil {
		p.StoragePolicy = new(Ydb_Table.StoragePolicy)
	}
	for _, opt := range opts {
		opt((*storagePolicy)(p.StoragePolicy))
	}
}

func WithStoragePolicy(opts ...StoragePolicyOption) ProfileOption {
	return storagePolicyProfileOption(opts)
}

type compactionPolicyProfileOption []CompactionPolicyOption

func (opts compactionPolicyProfileOption) ApplyProfileOption(p *profile, a *allocator.Allocator) {
	if p.CompactionPolicy == nil {
		p.CompactionPolicy = new(Ydb_Table.CompactionPolicy)
	}
	for _, opt := range opts {
		opt((*compactionPolicy)(p.CompactionPolicy))
	}
}

func WithCompactionPolicy(opts ...CompactionPolicyOption) ProfileOption {
	return compactionPolicyProfileOption(opts)
}

type partitioningPolicyProfileOption []PartitioningPolicyOption

func (opts partitioningPolicyProfileOption) ApplyProfileOption(p *profile, a *allocator.Allocator) {
	if p.PartitioningPolicy == nil {
		p.PartitioningPolicy = new(Ydb_Table.PartitioningPolicy)
	}
	for _, opt := range opts {
		opt((*partitioningPolicy)(p.PartitioningPolicy), a)
	}
}

func WithPartitioningPolicy(opts ...PartitioningPolicyOption) ProfileOption {
	return partitioningPolicyProfileOption(opts)
}

type executionPolicyProfileOption []ExecutionPolicyOption

func (opts executionPolicyProfileOption) ApplyProfileOption(p *profile, a *allocator.Allocator) {
	if p.ExecutionPolicy == nil {
		p.ExecutionPolicy = new(Ydb_Table.ExecutionPolicy)
	}
	for _, opt := range opts {
		opt((*executionPolicy)(p.ExecutionPolicy))
	}
}

func WithExecutionPolicy(opts ...ExecutionPolicyOption) ProfileOption {
	return executionPolicyProfileOption(opts)
}

type replicationPolicyProfileOption []ReplicationPolicyOption

func (opts replicationPolicyProfileOption) ApplyProfileOption(p *profile, a *allocator.Allocator) {
	if p.ReplicationPolicy == nil {
		p.ReplicationPolicy = new(Ydb_Table.ReplicationPolicy)
	}
	for _, opt := range opts {
		opt((*replicationPolicy)(p.ReplicationPolicy))
	}
}

func WithReplicationPolicy(opts ...ReplicationPolicyOption) ProfileOption {
	return replicationPolicyProfileOption(opts)
}

type cachingPolicyProfileOption []CachingPolicyOption

func (opts cachingPolicyProfileOption) ApplyProfileOption(p *profile, a *allocator.Allocator) {
	if p.CachingPolicy == nil {
		p.CachingPolicy = new(Ydb_Table.CachingPolicy)
	}
	for _, opt := range opts {
		opt((*cachingPolicy)(p.CachingPolicy))
	}
}

func WithCachingPolicy(opts ...CachingPolicyOption) ProfileOption {
	return cachingPolicyProfileOption(opts)
}

type (
	StoragePolicyOption      func(*storagePolicy)
	CompactionPolicyOption   func(*compactionPolicy)
	PartitioningPolicyOption func(*partitioningPolicy, *allocator.Allocator)
	ExecutionPolicyOption    func(*executionPolicy)
	ReplicationPolicyOption  func(*replicationPolicy)
	CachingPolicyOption      func(*cachingPolicy)
)

func WithStoragePolicyPreset(name string) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.PresetName = name
	}
}

func WithStoragePolicySyslog(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.Syslog = &Ydb_Table.StoragePool{Media: kind}
	}
}

func WithStoragePolicyLog(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.Log = &Ydb_Table.StoragePool{Media: kind}
	}
}

func WithStoragePolicyData(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.Data = &Ydb_Table.StoragePool{Media: kind}
	}
}

func WithStoragePolicyExternal(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.External = &Ydb_Table.StoragePool{Media: kind}
	}
}

func WithStoragePolicyKeepInMemory(flag FeatureFlag) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.KeepInMemory = flag.ToYDB()
	}
}

func WithCompactionPolicyPreset(name string) CompactionPolicyOption {
	return func(c *compactionPolicy) { c.PresetName = name }
}

func WithPartitioningPolicyPreset(name string) PartitioningPolicyOption {
	return func(p *partitioningPolicy, a *allocator.Allocator) {
		p.PresetName = name
	}
}

func WithPartitioningPolicyMode(mode PartitioningMode) PartitioningPolicyOption {
	return func(p *partitioningPolicy, a *allocator.Allocator) {
		p.AutoPartitioning = mode.toYDB()
	}
}

func WithPartitioningPolicyUniformPartitions(n uint64) PartitioningPolicyOption {
	return func(p *partitioningPolicy, a *allocator.Allocator) {
		p.Partitions = &Ydb_Table.PartitioningPolicy_UniformPartitions{
			UniformPartitions: n,
		}
	}
}

func WithPartitioningPolicyExplicitPartitions(splitPoints ...types.Value) PartitioningPolicyOption {
	return func(p *partitioningPolicy, a *allocator.Allocator) {
		values := make([]*Ydb.TypedValue, len(splitPoints))
		for i := range values {
			values[i] = value.ToYDB(splitPoints[i], a)
		}
		p.Partitions = &Ydb_Table.PartitioningPolicy_ExplicitPartitions{
			ExplicitPartitions: &Ydb_Table.ExplicitPartitions{
				SplitPoints: values,
			},
		}
	}
}

func WithReplicationPolicyPreset(name string) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		e.PresetName = name
	}
}

func WithReplicationPolicyReplicasCount(n uint32) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		e.ReplicasCount = n
	}
}

func WithReplicationPolicyCreatePerAZ(flag FeatureFlag) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		e.CreatePerAvailabilityZone = flag.ToYDB()
	}
}

func WithReplicationPolicyAllowPromotion(flag FeatureFlag) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		e.AllowPromotion = flag.ToYDB()
	}
}

func WithExecutionPolicyPreset(name string) ExecutionPolicyOption {
	return func(e *executionPolicy) {
		e.PresetName = name
	}
}

func WithCachingPolicyPreset(name string) CachingPolicyOption {
	return func(e *cachingPolicy) {
		e.PresetName = name
	}
}

type partitioningSettingsObject PartitioningSettings

func (ps partitioningSettingsObject) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.AlterPartitioningSettings = PartitioningSettings(ps).toYDB()
}

func (ps partitioningSettingsObject) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	d.PartitioningSettings = PartitioningSettings(ps).toYDB()
}

func WithPartitioningSettingsObject(ps PartitioningSettings) CreateTableOption {
	return partitioningSettingsObject(ps)
}

type partitioningSettings []PartitioningSettingsOption

func (opts partitioningSettings) ApplyCreateTableOption(d *CreateTableDesc, a *allocator.Allocator) {
	settings := &ydbPartitioningSettings{}
	for _, opt := range opts {
		opt.ApplyPartitioningSettingsOption(settings)
	}
	d.PartitioningSettings = (*Ydb_Table.PartitioningSettings)(settings)
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
	settings.PartitioningBySize = FeatureFlag(flag).ToYDB()
}

func WithPartitioningBySize(flag FeatureFlag) PartitioningSettingsOption {
	return partitioningBySizePartitioningSettingsOption(flag)
}

type partitionSizeMbPartitioningSettingsOption uint64

func (partitionSizeMb partitionSizeMbPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	settings.PartitionSizeMb = uint64(partitionSizeMb)
}

func WithPartitionSizeMb(partitionSizeMb uint64) PartitioningSettingsOption {
	return partitionSizeMbPartitioningSettingsOption(partitionSizeMb)
}

type partitioningByLoadPartitioningSettingsOption FeatureFlag

func (flag partitioningByLoadPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	settings.PartitioningByLoad = FeatureFlag(flag).ToYDB()
}

func WithPartitioningByLoad(flag FeatureFlag) PartitioningSettingsOption {
	return partitioningByLoadPartitioningSettingsOption(flag)
}

type minPartitionsCountPartitioningSettingsOption uint64

func (minPartitionsCount minPartitionsCountPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	settings.MinPartitionsCount = uint64(minPartitionsCount)
}

func WithMinPartitionsCount(minPartitionsCount uint64) PartitioningSettingsOption {
	return minPartitionsCountPartitioningSettingsOption(minPartitionsCount)
}

type maxPartitionsCountPartitioningSettingsOption uint64

func (maxPartitionsCount maxPartitionsCountPartitioningSettingsOption) ApplyPartitioningSettingsOption(
	settings *ydbPartitioningSettings,
) {
	settings.MaxPartitionsCount = uint64(maxPartitionsCount)
}

func WithMaxPartitionsCount(maxPartitionsCount uint64) PartitioningSettingsOption {
	return maxPartitionsCountPartitioningSettingsOption(maxPartitionsCount)
}

type (
	DropTableDesc   Ydb_Table.DropTableRequest
	DropTableOption interface {
		ApplyDropTableOption(*DropTableDesc)
	}
)

type (
	AlterTableDesc   Ydb_Table.AlterTableRequest
	AlterTableOption interface {
		ApplyAlterTableOption(*AlterTableDesc, *allocator.Allocator)
	}
)

func WithAddColumn(name string, typ types.Type) AlterTableOption {
	return column{
		name: name,
		typ:  typ,
	}
}

func WithAlterAttribute(key, value string) AlterTableOption {
	return attribute{
		key:   key,
		value: value,
	}
}

func WithAddAttribute(key, value string) AlterTableOption {
	return attribute{
		key:   key,
		value: value,
	}
}

func WithDropAttribute(key string) AlterTableOption {
	return attribute{
		key: key,
	}
}

func WithAddColumnMeta(column Column) AlterTableOption {
	return columnMeta(column)
}

type dropColumn string

func (name dropColumn) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.DropColumns = append(d.DropColumns, string(name))
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

func WithSetTimeToLiveSettings(settings TimeToLiveSettings) AlterTableOption {
	return timeToLiveSettings(settings)
}

type dropTimeToLive struct{}

func (dropTimeToLive) ApplyAlterTableOption(d *AlterTableDesc, a *allocator.Allocator) {
	d.TtlAction = &Ydb_Table.AlterTableRequest_DropTtlSettings{}
}

func WithDropTimeToLive() AlterTableOption {
	return dropTimeToLive{}
}

type (
	CopyTableDesc   Ydb_Table.CopyTableRequest
	CopyTableOption func(*CopyTableDesc)
)

type (
	ExecuteSchemeQueryDesc   Ydb_Table.ExecuteSchemeQueryRequest
	ExecuteSchemeQueryOption func(*ExecuteSchemeQueryDesc)
)

type (
	ExecuteDataQueryDesc   Ydb_Table.ExecuteDataQueryRequest
	ExecuteDataQueryOption func(*ExecuteDataQueryDesc)
)

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

// WithQueryCachePolicyKeepInCache manages keep-in-cache policy
//
// Deprecated: data queries always executes with enabled keep-in-cache policy.
// Use WithKeepInCache for disabling keep-in-cache policy
func WithQueryCachePolicyKeepInCache() QueryCachePolicyOption {
	return withQueryCachePolicyKeepInCache(true)
}

func withQueryCachePolicyKeepInCache(keepInCache bool) QueryCachePolicyOption {
	return func(p *queryCachePolicy) {
		p.KeepInCache = keepInCache
	}
}

// WithQueryCachePolicy manages query cache policy
//
// Deprecated: use WithKeepInCache for disabling keep-in-cache policy
func WithQueryCachePolicy(opts ...QueryCachePolicyOption) ExecuteDataQueryOption {
	return withQueryCachePolicy(opts...)
}

func withQueryCachePolicy(opts ...QueryCachePolicyOption) ExecuteDataQueryOption {
	return func(d *ExecuteDataQueryDesc) {
		if d.QueryCachePolicy == nil {
			d.QueryCachePolicy = &Ydb_Table.QueryCachePolicy{
				KeepInCache: true,
			}
		}
		for _, opt := range opts {
			opt((*queryCachePolicy)(d.QueryCachePolicy))
		}
	}
}

func WithCommitCollectStatsModeNone() CommitTransactionOption {
	return func(d *CommitTransactionDesc) {
		d.CollectStats = Ydb_Table.QueryStatsCollection_STATS_COLLECTION_NONE
	}
}

func WithCommitCollectStatsModeBasic() CommitTransactionOption {
	return func(d *CommitTransactionDesc) {
		d.CollectStats = Ydb_Table.QueryStatsCollection_STATS_COLLECTION_BASIC
	}
}

func WithCollectStatsModeNone() ExecuteDataQueryOption {
	return func(d *ExecuteDataQueryDesc) {
		d.CollectStats = Ydb_Table.QueryStatsCollection_STATS_COLLECTION_NONE
	}
}

func WithCollectStatsModeBasic() ExecuteDataQueryOption {
	return func(d *ExecuteDataQueryDesc) {
		d.CollectStats = Ydb_Table.QueryStatsCollection_STATS_COLLECTION_BASIC
	}
}

type (
	ExecuteScanQueryDesc   Ydb_Table.ExecuteScanQueryRequest
	ExecuteScanQueryOption func(*ExecuteScanQueryDesc)
)

// WithExecuteScanQueryMode defines scan query mode: execute or explain
func WithExecuteScanQueryMode(m ExecuteScanQueryRequestMode) ExecuteScanQueryOption {
	return func(desc *ExecuteScanQueryDesc) {
		desc.Mode = m.toYDB()
	}
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
	return func(desc *ExecuteScanQueryDesc) {
		desc.CollectStats = stats.toYDB()
	}
}

// Read table options
type (
	ReadTableDesc   Ydb_Table.ReadTableRequest
	ReadTableOption func(*ReadTableDesc, *allocator.Allocator)
)

func ReadColumn(name string) ReadTableOption {
	return func(desc *ReadTableDesc, a *allocator.Allocator) {
		desc.Columns = append(desc.Columns, name)
	}
}

func ReadOrdered() ReadTableOption {
	return func(desc *ReadTableDesc, a *allocator.Allocator) {
		desc.Ordered = true
	}
}

// ReadKeyRange returns ReadTableOption which makes ReadTable read values
// in range [x.From, x.To).
//
// Both x.From and x.To may be nil.
func ReadKeyRange(x KeyRange) ReadTableOption {
	return func(desc *ReadTableDesc, a *allocator.Allocator) {
		if x.From != nil {
			ReadGreaterOrEqual(x.From)(desc, a)
		}
		if x.To != nil {
			ReadLess(x.To)(desc, a)
		}
	}
}

func ReadGreater(x types.Value) ReadTableOption {
	return func(desc *ReadTableDesc, a *allocator.Allocator) {
		desc.initKeyRange()
		desc.KeyRange.FromBound = &Ydb_Table.KeyRange_Greater{
			Greater: value.ToYDB(x, a),
		}
	}
}

func ReadGreaterOrEqual(x types.Value) ReadTableOption {
	return func(desc *ReadTableDesc, a *allocator.Allocator) {
		desc.initKeyRange()
		desc.KeyRange.FromBound = &Ydb_Table.KeyRange_GreaterOrEqual{
			GreaterOrEqual: value.ToYDB(x, a),
		}
	}
}

func ReadLess(x types.Value) ReadTableOption {
	return func(desc *ReadTableDesc, a *allocator.Allocator) {
		desc.initKeyRange()
		desc.KeyRange.ToBound = &Ydb_Table.KeyRange_Less{
			Less: value.ToYDB(x, a),
		}
	}
}

func ReadLessOrEqual(x types.Value) ReadTableOption {
	return func(desc *ReadTableDesc, a *allocator.Allocator) {
		desc.initKeyRange()
		desc.KeyRange.ToBound = &Ydb_Table.KeyRange_LessOrEqual{
			LessOrEqual: value.ToYDB(x, a),
		}
	}
}

func ReadRowLimit(n uint64) ReadTableOption {
	return func(desc *ReadTableDesc, a *allocator.Allocator) {
		desc.RowLimit = n
	}
}

func (d *ReadTableDesc) initKeyRange() {
	if d.KeyRange == nil {
		d.KeyRange = new(Ydb_Table.KeyRange)
	}
}
