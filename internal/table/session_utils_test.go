package table

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
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
