package table

import (
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Table"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
)

type SessionStatus uint

const (
	SessionStatusUnknown SessionStatus = iota
	SessionReady
	SessionBusy
)

func (s SessionStatus) String() string {
	switch s {
	case SessionReady:
		return "ready"
	case SessionBusy:
		return "busy"
	default:
		return "unknown"
	}
}

type SessionInfo struct {
	Status SessionStatus
}

type Column struct {
	Name string
	Type ydb.Type
}

type Description struct {
	Name       string
	Columns    []Column
	PrimaryKey []string
	KeyRanges  []KeyRange
}

type (
	describeTableDesc   Ydb_Table.DescribeTableRequest
	DescribeTableOption func(d *describeTableDesc)
)

func WithShardKeyBounds() DescribeTableOption {
	return func(d *describeTableDesc) {
		d.IncludeShardKeyBounds = true
	}
}

func WithTableStats() DescribeTableOption {
	return func(d *describeTableDesc) {
		d.IncludeTableStats = true
	}
}

func WithPartitionStats() DescribeTableOption {
	return func(d *describeTableDesc) {
		d.IncludePartitionStats = true
	}
}

type (
	createTableDesc   Ydb_Table.CreateTableRequest
	CreateTableOption func(d *createTableDesc)
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

func WithColumn(name string, typ ydb.Type) CreateTableOption {
	return func(d *createTableDesc) {
		d.Columns = append(d.Columns, &Ydb_Table.ColumnMeta{
			Name: name,
			Type: internal.TypeToYDB(typ),
		})
	}
}

func WithPrimaryKeyColumn(columns ...string) CreateTableOption {
	return func(d *createTableDesc) {
		d.PrimaryKey = append(d.PrimaryKey, columns...)
	}
}

type (
	indexDesc   Ydb_Table.TableIndex
	IndexOption func(d *indexDesc)
)

func WithIndex(name string, opts ...IndexOption) CreateTableOption {
	return func(d *createTableDesc) {
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

type IndexType interface {
	setup(*indexDesc)
}

type globalIndex struct{}

func GlobalIndex() IndexType {
	return globalIndex{}
}

func (globalIndex) setup(d *indexDesc) {
	d.Type = &Ydb_Table.TableIndex_GlobalIndex{
		GlobalIndex: new(Ydb_Table.GlobalIndex),
	}
}

func WithIndexType(t IndexType) IndexOption {
	return func(d *indexDesc) {
		t.setup(d)
	}
}

func WithProfile(opts ...ProfileOption) CreateTableOption {
	return func(d *createTableDesc) {
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
		s.Syslog = &Ydb_Table.StorageSettings{StorageKind: kind}
	}
}
func WithStoragePolicyLog(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.Log = &Ydb_Table.StorageSettings{StorageKind: kind}
	}
}
func WithStoragePolicyData(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.Data = &Ydb_Table.StorageSettings{StorageKind: kind}
	}
}
func WithStoragePolicyExternal(kind string) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.External = &Ydb_Table.StorageSettings{StorageKind: kind}
	}
}
func WithStoragePolicyKeepInMemory(flag ydb.FeatureFlag) StoragePolicyOption {
	return func(s *storagePolicy) {
		s.KeepInMemory = internal.FeatureFlagToYDB(flag)
	}
}

func WithCompactionPolicyPreset(name string) CompactionPolicyOption {
	return func(c *compactionPolicy) { c.PresetName = name }
}

type PartitioningMode uint

func (p PartitioningMode) toYDB() Ydb_Table.PartitioningPolicy_AutoPartitioningPolicy {
	switch p {
	case PartitioningDisabled:
		return Ydb_Table.PartitioningPolicy_DISABLED
	case PartitioningAutoSplit:
		return Ydb_Table.PartitioningPolicy_AUTO_SPLIT
	case PartitioningAutoSplitMerge:
		return Ydb_Table.PartitioningPolicy_AUTO_SPLIT_MERGE
	default:
		panic("ydb: unknown partitioning mode")
	}
}

const (
	PartitioningUnknown PartitioningMode = iota
	PartitioningDisabled
	PartitioningAutoSplit
	PartitioningAutoSplitMerge
)

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
func WithPartitioningPolicyExplicitPartitions(splitPoints ...ydb.Value) PartitioningPolicyOption {
	return func(p *partitioningPolicy) {
		values := make([]*Ydb.TypedValue, len(splitPoints))
		for i := range values {
			values[i] = internal.ValueToYDB(splitPoints[i])
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
func WithReplicationPolicyCreatePerAZ(flag ydb.FeatureFlag) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		e.CreatePerAvailabilityZone = internal.FeatureFlagToYDB(flag)
	}
}
func WithReplicationPolicyAllowPromotion(flag ydb.FeatureFlag) ReplicationPolicyOption {
	return func(e *replicationPolicy) {
		e.AllowPromotion = internal.FeatureFlagToYDB(flag)
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

type (
	dropTableDesc   Ydb_Table.DropTableRequest
	DropTableOption func(*dropTableDesc)
)

type (
	alterTableDesc   Ydb_Table.AlterTableRequest
	AlterTableOption func(*alterTableDesc)
)

func WithAddColumn(name string, typ ydb.Type) AlterTableOption {
	return func(d *alterTableDesc) {
		d.AddColumns = append(d.AddColumns, &Ydb_Table.ColumnMeta{
			Name: name,
			Type: internal.TypeToYDB(typ),
		})
	}
}

func WithDropColumn(name string) AlterTableOption {
	return func(d *alterTableDesc) {
		d.DropColumns = append(d.DropColumns, name)
	}
}

type (
	copyTableDesc   Ydb_Table.CopyTableRequest
	CopyTableOption func(*copyTableDesc)
)

type (
	txDesc   Ydb_Table.TransactionSettings
	TxOption func(*txDesc)
)

type TransactionSettings struct {
	settings Ydb_Table.TransactionSettings
}

func TxSettings(opts ...TxOption) *TransactionSettings {
	s := new(TransactionSettings)
	for _, opt := range opts {
		opt((*txDesc)(&s.settings))
	}
	return s
}

func BeginTx(opts ...TxOption) TxControlOption {
	return func(d *txControlDesc) {
		s := TxSettings(opts...)
		d.TxSelector = &Ydb_Table.TransactionControl_BeginTx{
			BeginTx: &s.settings,
		}
	}
}

func WithTx(t *Transaction) TxControlOption {
	return func(d *txControlDesc) {
		d.TxSelector = &Ydb_Table.TransactionControl_TxId{
			TxId: t.id,
		}
	}
}

func CommitTx() TxControlOption {
	return func(d *txControlDesc) {
		d.CommitTx = true
	}
}

var (
	serializableReadWrite = &Ydb_Table.TransactionSettings_SerializableReadWrite{
		SerializableReadWrite: &Ydb_Table.SerializableModeSettings{},
	}
	staleReadOnly = &Ydb_Table.TransactionSettings_StaleReadOnly{
		StaleReadOnly: &Ydb_Table.StaleModeSettings{},
	}
)

func WithSerializableReadWrite() TxOption {
	return func(d *txDesc) {
		d.TxMode = serializableReadWrite
	}
}

func WithStaleReadOnly() TxOption {
	return func(d *txDesc) {
		d.TxMode = staleReadOnly
	}
}

func WithOnlineReadOnly(opts ...TxOnlineReadOnlyOption) TxOption {
	return func(d *txDesc) {
		var ro txOnlineReadOnly
		for _, opt := range opts {
			opt(&ro)
		}
		d.TxMode = &Ydb_Table.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: (*Ydb_Table.OnlineModeSettings)(&ro),
		}
	}
}

type txOnlineReadOnly Ydb_Table.OnlineModeSettings

type TxOnlineReadOnlyOption func(*txOnlineReadOnly)

func WithInconsistentReads() TxOnlineReadOnlyOption {
	return func(d *txOnlineReadOnly) {
		d.AllowInconsistentReads = true
	}
}

type (
	txControlDesc   Ydb_Table.TransactionControl
	TxControlOption func(*txControlDesc)
)

type TransactionControl struct {
	desc Ydb_Table.TransactionControl
}

func (t TransactionControl) id() string {
	if tx, ok := t.desc.TxSelector.(*Ydb_Table.TransactionControl_TxId); ok {
		return tx.TxId
	}
	return ""
}

func TxControl(opts ...TxControlOption) *TransactionControl {
	c := new(TransactionControl)
	for _, opt := range opts {
		opt((*txControlDesc)(&c.desc))
	}
	return c
}

type TableOptionsDescription struct {
	TableProfilePresets       []TableProfileDescription
	StoragePolicyPresets      []StoragePolicyDescription
	CompactionPolicyPresets   []CompactionPolicyDescription
	PartitioningPolicyPresets []PartitioningPolicyDescription
	ExecutionPolicyPresets    []ExecutionPolicyDescription
	ReplicationPolicyPresets  []ReplicationPolicyDescription
	CachingPolicyPresets      []CachingPolicyDescription
}

type (
	TableProfileDescription struct {
		Name   string
		Labels map[string]string

		DefaultStoragePolicy      string
		DefaultCompactionPolicy   string
		DefaultPartitioningPolicy string
		DefaultExecutionPolicy    string
		DefaultReplicationPolicy  string
		DefaultCachingPolicy      string

		AllowedStoragePolicies      []string
		AllowedCompactionPolicies   []string
		AllowedPartitioningPolicies []string
		AllowedExecutionPolicies    []string
		AllowedReplicationPolicies  []string
		AllowedCachingPolicies      []string
	}
	StoragePolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	CompactionPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	PartitioningPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	ExecutionPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	ReplicationPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
	CachingPolicyDescription struct {
		Name   string
		Labels map[string]string
	}
)

type (
	executeDataQueryDesc   Ydb_Table.ExecuteDataQueryRequest
	ExecuteDataQueryOption func(*executeDataQueryDesc)
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
	return func(d *executeDataQueryDesc) {
		if d.QueryCachePolicy == nil {
			d.QueryCachePolicy = new(Ydb_Table.QueryCachePolicy)
		}
		for _, opt := range opts {
			opt((*queryCachePolicy)(d.QueryCachePolicy))
		}
	}
}

func WithCollectStatsModeNone() ExecuteDataQueryOption {
	return func(d *executeDataQueryDesc) {
		d.CollectStats = Ydb_Table.ExecuteDataQueryRequest_STATS_COLLECTION_NONE
	}
}

func WithCollectStatsModeBasic() ExecuteDataQueryOption {
	return func(d *executeDataQueryDesc) {
		d.CollectStats = Ydb_Table.ExecuteDataQueryRequest_STATS_COLLECTION_BASIC
	}
}

type (
	executeSchemeQueryDesc   Ydb_Table.ExecuteSchemeQueryRequest
	ExecuteSchemeQueryOption func(*executeSchemeQueryDesc)
)

type (
	readTableDesc   Ydb_Table.ReadTableRequest
	ReadTableOption func(*readTableDesc)
)

type KeyRange struct {
	From ydb.Value
	To   ydb.Value
}

func (d *readTableDesc) initKeyRange() {
	if d.KeyRange == nil {
		d.KeyRange = new(Ydb_Table.KeyRange)
	}
}

func ReadColumn(name string) ReadTableOption {
	return func(desc *readTableDesc) {
		desc.Columns = append(desc.Columns, name)
	}
}
func ReadOrdered() ReadTableOption {
	return func(desc *readTableDesc) {
		desc.Ordered = true
	}
}

// ReadKeyRange returns ReadTableOption which makes ReadTable read values
// in range [x.From, x.To).
//
// Both x.From and x.To may be nil.
func ReadKeyRange(x KeyRange) ReadTableOption {
	return func(desc *readTableDesc) {
		desc.initKeyRange()
		if x.From != nil {
			desc.KeyRange.FromBound = &Ydb_Table.KeyRange_GreaterOrEqual{
				GreaterOrEqual: internal.ValueToYDB(x.From),
			}
		}
		if x.To != nil {
			desc.KeyRange.ToBound = &Ydb_Table.KeyRange_Less{
				Less: internal.ValueToYDB(x.To),
			}
		}
	}
}

func ReadGreater(x ydb.Value) ReadTableOption {
	return func(desc *readTableDesc) {
		desc.initKeyRange()
		desc.KeyRange.FromBound = &Ydb_Table.KeyRange_Greater{
			Greater: internal.ValueToYDB(x),
		}
	}
}
func ReadGreaterOrEqual(x ydb.Value) ReadTableOption {
	return func(desc *readTableDesc) {
		desc.initKeyRange()
		desc.KeyRange.FromBound = &Ydb_Table.KeyRange_GreaterOrEqual{
			GreaterOrEqual: internal.ValueToYDB(x),
		}
	}
}
func ReadLess(x ydb.Value) ReadTableOption {
	return func(desc *readTableDesc) {
		desc.initKeyRange()
		desc.KeyRange.ToBound = &Ydb_Table.KeyRange_Less{
			Less: internal.ValueToYDB(x),
		}
	}
}
func ReadLessOrEqual(x ydb.Value) ReadTableOption {
	return func(desc *readTableDesc) {
		desc.initKeyRange()
		desc.KeyRange.ToBound = &Ydb_Table.KeyRange_LessOrEqual{
			LessOrEqual: internal.ValueToYDB(x),
		}
	}
}
func ReadRowLimit(n uint64) ReadTableOption {
	return func(desc *readTableDesc) {
		desc.RowLimit = n
	}
}
