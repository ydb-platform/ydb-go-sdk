// nolint:revive
package ydb_table_options

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
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
	CreateTableOption func(d *CreateTableDesc)
)

type (
	profile       Ydb_Table.TableProfile
	ProfileOption func(p *profile)
)

type (
	storagePolicy      Ydb_Table.StoragePolicy
	compactionPolicy   Ydb_Table.CompactionPolicy
	partitioningPolicy Ydb_Table.PartitioningPolicy
	executionPolicy    Ydb_Table.ExecutionPolicy
	replicationPolicy  Ydb_Table.ReplicationPolicy
	cachingPolicy      Ydb_Table.CachingPolicy
)

func WithColumn(name string, typ ydb_table_types.Type) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.Columns = append(d.Columns, &Ydb_Table.ColumnMeta{
			Name: name,
			Type: value.TypeToYDB(typ),
		})
	}
}

func WithColumnMeta(column Column) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.Columns = append(d.Columns, column.toYDB())
	}
}

func WithPrimaryKeyColumn(columns ...string) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.PrimaryKey = append(d.PrimaryKey, columns...)
	}
}

func WithTimeToLiveSettings(settings TimeToLiveSettings) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.TtlSettings = settings.ToYDB()
	}
}

func WithAttribute(key, value string) CreateTableOption {
	return func(d *CreateTableDesc) {
		if d.Attributes == nil {
			d.Attributes = make(map[string]string)
		}
		d.Attributes[key] = value
	}
}

type (
	indexDesc   Ydb_Table.TableIndex
	IndexOption func(d *indexDesc)
)

func WithIndex(name string, opts ...IndexOption) CreateTableOption {
	return func(d *CreateTableDesc) {
		x := &Ydb_Table.TableIndex{
			Name: name,
		}
		for _, opt := range opts {
			opt((*indexDesc)(x))
		}
		d.Indexes = append(d.Indexes, x)
	}
}

func WithIndexColumns(columns ...string) IndexOption {
	return func(d *indexDesc) {
		d.IndexColumns = append(d.IndexColumns, columns...)
	}
}

func WithIndexType(t IndexType) IndexOption {
	return func(d *indexDesc) {
		t.setup(d)
	}
}

func WithColumnFamilies(cf ...ColumnFamily) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.ColumnFamilies = make([]*Ydb_Table.ColumnFamily, len(cf))
		for i, c := range cf {
			d.ColumnFamilies[i] = c.toYDB()
		}
	}
}

func WithReadReplicasSettings(rr ReadReplicasSettings) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.ReadReplicasSettings = rr.ToYDB()
	}
}

func WithStorageSettings(ss StorageSettings) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.StorageSettings = ss.ToYDB()
	}
}

func WithKeyBloomFilter(f FeatureFlag) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.KeyBloomFilter = f.ToYDB()
	}
}

func WithProfile(opts ...ProfileOption) CreateTableOption {
	return func(d *CreateTableDesc) {
		if d.Profile == nil {
			d.Profile = new(Ydb_Table.TableProfile)
		}
		for _, opt := range opts {
			opt((*profile)(d.Profile))
		}
	}
}

func WithProfilePreset(name string) ProfileOption {
	return func(p *profile) {
		p.PresetName = name
	}
}

func WithStoragePolicy(opts ...StoragePolicyOption) ProfileOption {
	return func(p *profile) {
		if p.StoragePolicy == nil {
			p.StoragePolicy = new(Ydb_Table.StoragePolicy)
		}
		for _, opt := range opts {
			opt((*storagePolicy)(p.StoragePolicy))
		}
	}
}

func WithCompactionPolicy(opts ...CompactionPolicyOption) ProfileOption {
	return func(p *profile) {
		if p.CompactionPolicy == nil {
			p.CompactionPolicy = new(Ydb_Table.CompactionPolicy)
		}
		for _, opt := range opts {
			opt((*compactionPolicy)(p.CompactionPolicy))
		}
	}
}

func WithPartitioningPolicy(opts ...PartitioningPolicyOption) ProfileOption {
	return func(p *profile) {
		if p.PartitioningPolicy == nil {
			p.PartitioningPolicy = new(Ydb_Table.PartitioningPolicy)
		}
		for _, opt := range opts {
			opt((*partitioningPolicy)(p.PartitioningPolicy))
		}
	}
}

func WithExecutionPolicy(opts ...ExecutionPolicyOption) ProfileOption {
	return func(p *profile) {
		if p.ExecutionPolicy == nil {
			p.ExecutionPolicy = new(Ydb_Table.ExecutionPolicy)
		}
		for _, opt := range opts {
			opt((*executionPolicy)(p.ExecutionPolicy))
		}
	}
}

func WithReplicationPolicy(opts ...ReplicationPolicyOption) ProfileOption {
	return func(p *profile) {
		if p.ReplicationPolicy == nil {
			p.ReplicationPolicy = new(Ydb_Table.ReplicationPolicy)
		}
		for _, opt := range opts {
			opt((*replicationPolicy)(p.ReplicationPolicy))
		}
	}
}

func WithCachingPolicy(opts ...CachingPolicyOption) ProfileOption {
	return func(p *profile) {
		if p.CachingPolicy == nil {
			p.CachingPolicy = new(Ydb_Table.CachingPolicy)
		}
		for _, opt := range opts {
			opt((*cachingPolicy)(p.CachingPolicy))
		}
	}
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
	return func(p *partitioningPolicy) {
		p.PresetName = name
	}
}

func WithPartitioningPolicyMode(mode PartitioningMode) PartitioningPolicyOption {
	return func(p *partitioningPolicy) {
		p.AutoPartitioning = mode.toYDB()
	}
}

func WithPartitioningPolicyUniformPartitions(n uint64) PartitioningPolicyOption {
	return func(p *partitioningPolicy) {
		p.Partitions = &Ydb_Table.PartitioningPolicy_UniformPartitions{
			UniformPartitions: n,
		}
	}
}

func WithPartitioningPolicyExplicitPartitions(splitPoints ...ydb_table_types.Value) PartitioningPolicyOption {
	return func(p *partitioningPolicy) {
		values := make([]*Ydb.TypedValue, len(splitPoints))
		for i := range values {
			values[i] = value.ToYDB(splitPoints[i])
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

func WithPartitioningSettingsObject(ps PartitioningSettings) CreateTableOption {
	return func(d *CreateTableDesc) {
		d.PartitioningSettings = ps.toYDB()
	}
}

func WithPartitioningSettings(opts ...PartitioningSettingsOption) CreateTableOption {
	return func(d *CreateTableDesc) {
		settings := &ydbPartitioningSettings{}
		for _, o := range opts {
			o(settings)
		}
		d.PartitioningSettings = (*Ydb_Table.PartitioningSettings)(settings)
	}
}

type (
	ydbPartitioningSettings    Ydb_Table.PartitioningSettings
	PartitioningSettingsOption func(settings *ydbPartitioningSettings)
)

func WithPartitioningBySize(flag FeatureFlag) PartitioningSettingsOption {
	return func(settings *ydbPartitioningSettings) {
		settings.PartitioningBySize = flag.ToYDB()
	}
}

func WithPartitionSizeMb(partitionSizeMb uint64) PartitioningSettingsOption {
	return func(settings *ydbPartitioningSettings) {
		settings.PartitionSizeMb = partitionSizeMb
	}
}

func WithPartitioningByLoad(flag FeatureFlag) PartitioningSettingsOption {
	return func(settings *ydbPartitioningSettings) {
		settings.PartitioningByLoad = flag.ToYDB()
	}
}

func WithMinPartitionsCount(minPartitionsCount uint64) PartitioningSettingsOption {
	return func(settings *ydbPartitioningSettings) {
		settings.MinPartitionsCount = minPartitionsCount
	}
}

func WithMaxPartitionsCount(maxPartitionsCount uint64) PartitioningSettingsOption {
	return func(settings *ydbPartitioningSettings) {
		settings.MaxPartitionsCount = maxPartitionsCount
	}
}

type (
	DropTableDesc   Ydb_Table.DropTableRequest
	DropTableOption func(*DropTableDesc)
)

type (
	AlterTableDesc   Ydb_Table.AlterTableRequest
	AlterTableOption func(*AlterTableDesc)
)

func WithAddColumn(name string, typ ydb_table_types.Type) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.AddColumns = append(d.AddColumns, &Ydb_Table.ColumnMeta{
			Name: name,
			Type: value.TypeToYDB(typ),
		})
	}
}

func WithAlterAttribute(key, value string) AlterTableOption {
	return func(d *AlterTableDesc) {
		if d.AlterAttributes == nil {
			d.AlterAttributes = make(map[string]string)
		}
		d.AlterAttributes[key] = value
	}
}

func WithAddColumnMeta(column Column) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.AddColumns = append(d.AddColumns, column.toYDB())
	}
}

func WithDropColumn(name string) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.DropColumns = append(d.DropColumns, name)
	}
}

func WithAddColumnFamilies(cf ...ColumnFamily) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.AddColumnFamilies = make([]*Ydb_Table.ColumnFamily, len(cf))
		for i, c := range cf {
			d.AddColumnFamilies[i] = c.toYDB()
		}
	}
}

func WithAlterColumnFamilies(cf ...ColumnFamily) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.AddColumnFamilies = make([]*Ydb_Table.ColumnFamily, len(cf))
		for i, c := range cf {
			d.AddColumnFamilies[i] = c.toYDB()
		}
	}
}

func WithAlterReadReplicasSettings(rr ReadReplicasSettings) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.SetReadReplicasSettings = rr.ToYDB()
	}
}

func WithAlterStorageSettings(ss StorageSettings) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.AlterStorageSettings = ss.ToYDB()
	}
}

func WithAlterKeyBloomFilter(f FeatureFlag) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.SetKeyBloomFilter = f.ToYDB()
	}
}

func WithAlterPartitionSettingsObject(ps PartitioningSettings) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.AlterPartitioningSettings = ps.toYDB()
	}
}

func WithSetTimeToLiveSettings(settings TimeToLiveSettings) AlterTableOption {
	return func(d *AlterTableDesc) {
		d.TtlAction = &Ydb_Table.AlterTableRequest_SetTtlSettings{
			SetTtlSettings: settings.ToYDB(),
		}
	}
}

func WithDropTimeToLive() AlterTableOption {
	return func(d *AlterTableDesc) {
		d.TtlAction = &Ydb_Table.AlterTableRequest_DropTtlSettings{}
	}
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

func WithQueryCachePolicyKeepInCache() QueryCachePolicyOption {
	return func(p *queryCachePolicy) {
		p.KeepInCache = true
	}
}

func WithQueryCachePolicy(opts ...QueryCachePolicyOption) ExecuteDataQueryOption {
	return func(d *ExecuteDataQueryDesc) {
		if d.QueryCachePolicy == nil {
			d.QueryCachePolicy = new(Ydb_Table.QueryCachePolicy)
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

func WithExecuteScanQueryMode(m ExecuteScanQueryRequestMode) ExecuteScanQueryOption {
	return func(desc *ExecuteScanQueryDesc) {
		desc.Mode = m.toYDB()
	}
}

// Read table options
type (
	ReadTableDesc   Ydb_Table.ReadTableRequest
	ReadTableOption func(*ReadTableDesc)
)

func ReadColumn(name string) ReadTableOption {
	return func(desc *ReadTableDesc) {
		desc.Columns = append(desc.Columns, name)
	}
}

func ReadOrdered() ReadTableOption {
	return func(desc *ReadTableDesc) {
		desc.Ordered = true
	}
}

// ReadKeyRange returns ReadTableOption which makes ReadTable read values
// in range [x.From, x.To).
//
// Both x.From and x.To may be nil.
func ReadKeyRange(x KeyRange) ReadTableOption {
	return func(desc *ReadTableDesc) {
		if x.From != nil {
			ReadGreaterOrEqual(x.From)(desc)
		}
		if x.To != nil {
			ReadLess(x.To)(desc)
		}
	}
}

func ReadGreater(x ydb_table_types.Value) ReadTableOption {
	return func(desc *ReadTableDesc) {
		desc.initKeyRange()
		desc.KeyRange.FromBound = &Ydb_Table.KeyRange_Greater{
			Greater: value.ToYDB(x),
		}
	}
}

func ReadGreaterOrEqual(x ydb_table_types.Value) ReadTableOption {
	return func(desc *ReadTableDesc) {
		desc.initKeyRange()
		desc.KeyRange.FromBound = &Ydb_Table.KeyRange_GreaterOrEqual{
			GreaterOrEqual: value.ToYDB(x),
		}
	}
}

func ReadLess(x ydb_table_types.Value) ReadTableOption {
	return func(desc *ReadTableDesc) {
		desc.initKeyRange()
		desc.KeyRange.ToBound = &Ydb_Table.KeyRange_Less{
			Less: value.ToYDB(x),
		}
	}
}

func ReadLessOrEqual(x ydb_table_types.Value) ReadTableOption {
	return func(desc *ReadTableDesc) {
		desc.initKeyRange()
		desc.KeyRange.ToBound = &Ydb_Table.KeyRange_LessOrEqual{
			LessOrEqual: value.ToYDB(x),
		}
	}
}

func ReadRowLimit(n uint64) ReadTableOption {
	return func(desc *ReadTableDesc) {
		desc.RowLimit = n
	}
}

func (d *ReadTableDesc) initKeyRange() {
	if d.KeyRange == nil {
		d.KeyRange = new(Ydb_Table.KeyRange)
	}
}
