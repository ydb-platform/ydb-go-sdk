package table

import (
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Table"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
)

type SessionStatus byte

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
	Name   string
	Type   ydb.Type
	Family string
}

func (c Column) toYDB() *Ydb_Table.ColumnMeta {
	return &Ydb_Table.ColumnMeta{
		Name:   c.Name,
		Type:   internal.TypeToYDB(c.Type),
		Family: c.Family,
	}
}

type Description struct {
	Name                 string
	Columns              []Column
	PrimaryKey           []string
	KeyRanges            []KeyRange
	Stats                *TableStats
	ColumnFamilies       []ColumnFamily
	Attributes           map[string]string
	ReadReplicaSettings  ReadReplicasSettings
	StorageSettings      StorageSettings
	KeyBloomFilter       ydb.FeatureFlag
	PartitioningSettings PartitioningSettings
	TTLSettings          *TTLSettings
}

type TableStats struct {
	PartitionStats   []PartitionStats
	RowsEstimate     uint64
	StoreSize        uint64
	Partitions       uint64
	CreationTime     time.Time
	ModificationTime time.Time
}

type PartitionStats struct {
	RowsEstimate uint64
	StoreSize    uint64
}

type ColumnFamily struct {
	Name         string
	Data         StoragePool
	Compression  ColumnFamilyCompression
	KeepInMemory ydb.FeatureFlag
}

func (c ColumnFamily) toYDB() *Ydb_Table.ColumnFamily {
	return &Ydb_Table.ColumnFamily{
		Name:         c.Name,
		Data:         c.Data.toYDB(),
		Compression:  c.Compression.toYDB(),
		KeepInMemory: c.KeepInMemory.ToYDB(),
	}
}

func columnFamily(c *Ydb_Table.ColumnFamily) ColumnFamily {
	return ColumnFamily{
		Name:         c.Name,
		Data:         storagePool(c.GetData()),
		Compression:  columnFamilyCompression(c.Compression),
		KeepInMemory: internal.FeatureFlagFromYDB(c.KeepInMemory),
	}
}

type StoragePool struct {
	Media string
}

func (s StoragePool) toYDB() *Ydb_Table.StoragePool {
	if s.Media == "" {
		return nil
	}
	return &Ydb_Table.StoragePool{
		Media: s.Media,
	}
}

func storagePool(s *Ydb_Table.StoragePool) StoragePool {
	return StoragePool{
		Media: s.GetMedia(),
	}
}

type ColumnFamilyCompression byte

const (
	ColumnFamilyCompressionUnknown ColumnFamilyCompression = iota
	ColumnFamilyCompressionNone
	ColumnFamilyCompressionLZ4
)

func (c ColumnFamilyCompression) String() string {
	switch c {
	case ColumnFamilyCompressionNone:
		return "none"
	case ColumnFamilyCompressionLZ4:
		return "lz4"
	default:
		return "unknown"
	}
}

func (c ColumnFamilyCompression) toYDB() Ydb_Table.ColumnFamily_Compression {
	switch c {
	case ColumnFamilyCompressionNone:
		return Ydb_Table.ColumnFamily_COMPRESSION_NONE
	case ColumnFamilyCompressionLZ4:
		return Ydb_Table.ColumnFamily_COMPRESSION_LZ4
	default:
		return Ydb_Table.ColumnFamily_COMPRESSION_UNSPECIFIED
	}
}

func columnFamilyCompression(c Ydb_Table.ColumnFamily_Compression) ColumnFamilyCompression {
	switch c {
	case Ydb_Table.ColumnFamily_COMPRESSION_NONE:
		return ColumnFamilyCompressionNone
	case Ydb_Table.ColumnFamily_COMPRESSION_LZ4:
		return ColumnFamilyCompressionLZ4
	default:
		return ColumnFamilyCompressionUnknown
	}
}

type (
	describeTableDesc   Ydb_Table.DescribeTableRequest
	DescribeTableOption func(d *describeTableDesc)
)

type ReadReplicasSettings struct {
	Type  ReadReplicasType
	Count uint64
}

func (rr ReadReplicasSettings) toYDB() *Ydb_Table.ReadReplicasSettings {
	switch rr.Type {
	case ReadReplicasPerAzReadReplicas:
		return &Ydb_Table.ReadReplicasSettings{
			Settings: &Ydb_Table.ReadReplicasSettings_PerAzReadReplicasCount{
				PerAzReadReplicasCount: rr.Count,
			},
		}

	default:
		return &Ydb_Table.ReadReplicasSettings{
			Settings: &Ydb_Table.ReadReplicasSettings_AnyAzReadReplicasCount{
				AnyAzReadReplicasCount: rr.Count,
			},
		}
	}
}

func readReplicasSettings(rr *Ydb_Table.ReadReplicasSettings) ReadReplicasSettings {
	t, c := ReadReplicasPerAzReadReplicas, uint64(0)

	if c = rr.GetPerAzReadReplicasCount(); c != 0 {
		t = ReadReplicasPerAzReadReplicas
	} else if c = rr.GetAnyAzReadReplicasCount(); c != 0 {
		t = ReadReplicasAnyAzReadReplicas
	}

	return ReadReplicasSettings{
		Type:  t,
		Count: c,
	}
}

type ReadReplicasType byte

const (
	ReadReplicasPerAzReadReplicas ReadReplicasType = iota
	ReadReplicasAnyAzReadReplicas
)

type StorageSettings struct {
	TableCommitLog0    StoragePool
	TableCommitLog1    StoragePool
	External           StoragePool
	StoreExternalBlobs ydb.FeatureFlag
}

func (ss StorageSettings) toYDB() *Ydb_Table.StorageSettings {
	return &Ydb_Table.StorageSettings{
		TabletCommitLog0:   ss.TableCommitLog0.toYDB(),
		TabletCommitLog1:   ss.TableCommitLog1.toYDB(),
		External:           ss.External.toYDB(),
		StoreExternalBlobs: ss.StoreExternalBlobs.ToYDB(),
	}
}

func storageSettings(ss *Ydb_Table.StorageSettings) StorageSettings {
	return StorageSettings{
		TableCommitLog0:    storagePool(ss.GetTabletCommitLog0()),
		TableCommitLog1:    storagePool(ss.GetTabletCommitLog1()),
		External:           storagePool(ss.GetExternal()),
		StoreExternalBlobs: internal.FeatureFlagFromYDB(ss.GetStoreExternalBlobs()),
	}
}

type PartitioningSettings struct {
	PartitioningBySize ydb.FeatureFlag
	PartitionSizeMb    uint64
	PartitioningByLoad ydb.FeatureFlag
	MinPartitionsCount uint64
	MaxPartitionsCount uint64
}

func (ps PartitioningSettings) toYDB() *Ydb_Table.PartitioningSettings {
	return &Ydb_Table.PartitioningSettings{
		PartitioningBySize: ps.PartitioningBySize.ToYDB(),
		PartitionSizeMb:    ps.PartitionSizeMb,
		PartitioningByLoad: ps.PartitioningByLoad.ToYDB(),
		MinPartitionsCount: ps.MinPartitionsCount,
		MaxPartitionsCount: ps.MaxPartitionsCount,
	}
}

func partitioningSettings(ps *Ydb_Table.PartitioningSettings) PartitioningSettings {
	return PartitioningSettings{
		PartitioningBySize: internal.FeatureFlagFromYDB(ps.GetPartitioningBySize()),
		PartitionSizeMb:    ps.GetPartitionSizeMb(),
		PartitioningByLoad: internal.FeatureFlagFromYDB(ps.GetPartitioningByLoad()),
		MinPartitionsCount: ps.GetMinPartitionsCount(),
		MaxPartitionsCount: ps.GetMaxPartitionsCount(),
	}
}

func ttlSettings(s *Ydb_Table.TtlSettings) *TTLSettings {
	if s == nil {
		return nil
	}
	switch mode := s.Mode.(type) {
	// for the time being the only implementation of Mode
	case *Ydb_Table.TtlSettings_DateTypeColumn:
		c := mode.DateTypeColumn
		return &TTLSettings{
			DateTimeColumn: c.ColumnName,
			TTLSeconds:     c.ExpireAfterSeconds,
		}
	default:
		return nil
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

type PartitioningMode byte

const (
	PartitioningUnknown PartitioningMode = iota
	PartitioningDisabled
	PartitioningAutoSplit
	PartitioningAutoSplitMerge
)

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

type ExecuteScanQueryRequestMode byte

const (
	ExecuteScanQueryRequestModeExec ExecuteScanQueryRequestMode = iota
	ExecuteScanQueryRequestModeExplain
)

func (p ExecuteScanQueryRequestMode) toYDB() Ydb_Table.ExecuteScanQueryRequest_Mode {
	switch p {
	case ExecuteScanQueryRequestModeExec:
		return Ydb_Table.ExecuteScanQueryRequest_MODE_EXEC
	case ExecuteScanQueryRequestModeExplain:
		return Ydb_Table.ExecuteScanQueryRequest_MODE_EXPLAIN
	default:
		panic("ydb: unknown execute scan query mode")
	}
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

type KeyRange struct {
	From ydb.Value
	To   ydb.Value
}

type TTLSettings struct {
	DateTimeColumn string
	TTLSeconds     uint32
}
