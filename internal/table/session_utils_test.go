package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestStatsModeToStatsMode(t *testing.T) {
	t.Run("STATS_COLLECTION_NONE", func(t *testing.T) {
		result := statsModeToStatsMode(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_NONE)
		require.Equal(t, Ydb_Query.StatsMode_STATS_MODE_NONE, result)
	})

	t.Run("STATS_COLLECTION_BASIC", func(t *testing.T) {
		result := statsModeToStatsMode(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_BASIC)
		require.Equal(t, Ydb_Query.StatsMode_STATS_MODE_BASIC, result)
	})

	t.Run("STATS_COLLECTION_FULL", func(t *testing.T) {
		result := statsModeToStatsMode(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_FULL)
		require.Equal(t, Ydb_Query.StatsMode_STATS_MODE_FULL, result)
	})

	t.Run("STATS_COLLECTION_PROFILE", func(t *testing.T) {
		result := statsModeToStatsMode(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_PROFILE)
		require.Equal(t, Ydb_Query.StatsMode_STATS_MODE_PROFILE, result)
	})

	t.Run("STATS_COLLECTION_UNSPECIFIED", func(t *testing.T) {
		result := statsModeToStatsMode(Ydb_Table.QueryStatsCollection_STATS_COLLECTION_UNSPECIFIED)
		require.Equal(t, Ydb_Query.StatsMode_STATS_MODE_UNSPECIFIED, result)
	})

	t.Run("unknown mode", func(t *testing.T) {
		result := statsModeToStatsMode(Ydb_Table.QueryStatsCollection_Mode(999))
		require.Equal(t, Ydb_Query.StatsMode_STATS_MODE_UNSPECIFIED, result)
	})
}

func TestProcessTableStats(t *testing.T) {
	t.Run("nil stats", func(t *testing.T) {
		result := processTableStats(nil)
		require.Nil(t, result)
	})

	t.Run("empty stats", func(t *testing.T) {
		stats := &Ydb_Table.TableStats{}
		result := processTableStats(stats)
		require.NotNil(t, result)
		require.Empty(t, result.PartitionStats)
		require.Zero(t, result.RowsEstimate)
		require.Zero(t, result.StoreSize)
		require.Zero(t, result.Partitions)
	})

	t.Run("stats with partitions", func(t *testing.T) {
		stats := &Ydb_Table.TableStats{
			PartitionStats: []*Ydb_Table.PartitionStats{
				{
					RowsEstimate: 100,
					StoreSize:    1024,
					LeaderNodeId: 1,
				},
				{
					RowsEstimate: 200,
					StoreSize:    2048,
					LeaderNodeId: 2,
				},
			},
			RowsEstimate: 300,
			StoreSize:    3072,
			Partitions:   2,
		}
		result := processTableStats(stats)
		require.NotNil(t, result)
		require.Len(t, result.PartitionStats, 2)
		require.Equal(t, uint64(100), result.PartitionStats[0].RowsEstimate)
		require.Equal(t, uint64(1024), result.PartitionStats[0].StoreSize)
		require.Equal(t, uint32(1), result.PartitionStats[0].LeaderNodeID)
		require.Equal(t, uint64(200), result.PartitionStats[1].RowsEstimate)
		require.Equal(t, uint64(2048), result.PartitionStats[1].StoreSize)
		require.Equal(t, uint32(2), result.PartitionStats[1].LeaderNodeID)
		require.Equal(t, uint64(300), result.RowsEstimate)
		require.Equal(t, uint64(3072), result.StoreSize)
		require.Equal(t, uint64(2), result.Partitions)
	})

	t.Run("stats with timestamps", func(t *testing.T) {
		creationTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		modificationTime := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)
		stats := &Ydb_Table.TableStats{
			CreationTime:     timestamppb.New(creationTime),
			ModificationTime: timestamppb.New(modificationTime),
		}
		result := processTableStats(stats)
		require.NotNil(t, result)
		require.Equal(t, creationTime.Unix(), result.CreationTime.Unix())
		require.Equal(t, modificationTime.Unix(), result.ModificationTime.Unix())
	})
}

func TestProcessColumnFamilies(t *testing.T) {
	t.Run("empty families", func(t *testing.T) {
		result := processColumnFamilies(nil)
		require.Empty(t, result)
	})

	t.Run("with families", func(t *testing.T) {
		families := []*Ydb_Table.ColumnFamily{
			{Name: "family1"},
			{Name: "family2"},
		}
		result := processColumnFamilies(families)
		require.Len(t, result, 2)
	})
}

func TestProcessAttributes(t *testing.T) {
	t.Run("nil attributes", func(t *testing.T) {
		result := processAttributes(nil)
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("empty attributes", func(t *testing.T) {
		result := processAttributes(map[string]string{})
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("with attributes", func(t *testing.T) {
		attrs := map[string]string{
			"key1": "value1",
			"key2": "value2",
		}
		result := processAttributes(attrs)
		require.NotNil(t, result)
		require.Len(t, result, 2)
		require.Equal(t, "value1", result["key1"])
		require.Equal(t, "value2", result["key2"])
	})
}

func TestProcessIndexes(t *testing.T) {
	t.Run("empty indexes", func(t *testing.T) {
		result := processIndexes(nil)
		require.Empty(t, result)
	})

	t.Run("global index", func(t *testing.T) {
		indexes := []*Ydb_Table.TableIndexDescription{
			{
				Name:         "idx1",
				IndexColumns: []string{"col1", "col2"},
				DataColumns:  []string{"col3"},
				Status:       Ydb_Table.TableIndexDescription_STATUS_READY,
				Type: &Ydb_Table.TableIndexDescription_GlobalIndex{
					GlobalIndex: &Ydb_Table.GlobalIndex{},
				},
			},
		}
		result := processIndexes(indexes)
		require.Len(t, result, 1)
		require.Equal(t, "idx1", result[0].Name)
		require.Equal(t, []string{"col1", "col2"}, result[0].IndexColumns)
		require.Equal(t, []string{"col3"}, result[0].DataColumns)
		require.Equal(t, Ydb_Table.TableIndexDescription_STATUS_READY, result[0].Status)
		require.Equal(t, options.IndexTypeGlobal, result[0].Type)
	})

	t.Run("global async index", func(t *testing.T) {
		indexes := []*Ydb_Table.TableIndexDescription{
			{
				Name:         "idx2",
				IndexColumns: []string{"col1"},
				Status:       Ydb_Table.TableIndexDescription_STATUS_READY,
				Type: &Ydb_Table.TableIndexDescription_GlobalAsyncIndex{
					GlobalAsyncIndex: &Ydb_Table.GlobalAsyncIndex{},
				},
			},
		}
		result := processIndexes(indexes)
		require.Len(t, result, 1)
		require.Equal(t, "idx2", result[0].Name)
		require.Equal(t, options.IndexTypeGlobalAsync, result[0].Type)
	})

	t.Run("multiple indexes", func(t *testing.T) {
		indexes := []*Ydb_Table.TableIndexDescription{
			{
				Name: "idx1",
				Type: &Ydb_Table.TableIndexDescription_GlobalIndex{
					GlobalIndex: &Ydb_Table.GlobalIndex{},
				},
			},
			{
				Name: "idx2",
				Type: &Ydb_Table.TableIndexDescription_GlobalAsyncIndex{
					GlobalAsyncIndex: &Ydb_Table.GlobalAsyncIndex{},
				},
			},
		}
		result := processIndexes(indexes)
		require.Len(t, result, 2)
		require.Equal(t, options.IndexTypeGlobal, result[0].Type)
		require.Equal(t, options.IndexTypeGlobalAsync, result[1].Type)
	})
}

func TestProcessChangefeeds(t *testing.T) {
	t.Run("empty changefeeds", func(t *testing.T) {
		result := processChangefeeds(nil)
		require.Empty(t, result)
	})

	t.Run("with changefeeds", func(t *testing.T) {
		changefeeds := []*Ydb_Table.ChangefeedDescription{
			{Name: "feed1"},
			{Name: "feed2"},
		}
		result := processChangefeeds(changefeeds)
		require.Len(t, result, 2)
	})
}
