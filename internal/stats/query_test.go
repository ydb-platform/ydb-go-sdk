package stats_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

func us(microseconds uint64) time.Duration {
	return time.Duration(microseconds) * time.Microsecond
}

func TestFromQueryStats(t *testing.T) {
	t.Run("NilProtoReturnsNil", func(t *testing.T) {
		require.Nil(t, stats.FromQueryStats(nil))
	})

	t.Run("WrapsProto", func(t *testing.T) {
		require.NotNil(t, stats.FromQueryStats(&Ydb_TableStats.QueryStats{}))
	})
}

func TestQueryStatsScalarFields(t *testing.T) {
	s := stats.FromQueryStats(sampleQueryStatsPB())

	require.Equal(t, us(100), s.ProcessCPUTime())
	require.Equal(t, us(200), s.TotalDuration())
	require.Equal(t, us(300), s.TotalCPUTime())
	require.Equal(t, "ast", s.QueryAST())
	require.Equal(t, "plan", s.QueryPlan())
	require.Equal(t, &stats.CompilationStats{
		FromCache: true,
		Duration:  us(123),
		CPUTime:   us(456),
	}, s.Compilation())
}

func TestQueryStatsNextPhase(t *testing.T) {
	t.Run("WalksAllPhasesAndStops", func(t *testing.T) {
		s := stats.FromQueryStats(sampleQueryStatsPB())

		phase1, ok := s.NextPhase()
		require.True(t, ok)
		require.NotNil(t, phase1)
		require.Equal(t, us(10), phase1.Duration())
		require.Equal(t, us(20), phase1.CPUTime())
		require.Equal(t, uint64(30), phase1.AffectedShards())
		require.True(t, phase1.IsLiteralPhase())

		phase2, ok := s.NextPhase()
		require.True(t, ok)
		require.NotNil(t, phase2)
		require.Equal(t, us(11), phase2.Duration())
		require.Equal(t, us(21), phase2.CPUTime())
		require.Equal(t, uint64(31), phase2.AffectedShards())
		require.False(t, phase2.IsLiteralPhase())

		end, ok := s.NextPhase()
		require.False(t, ok)
		require.Nil(t, end)
	})

	t.Run("ReturnsFalseOnEmpty", func(t *testing.T) {
		s := stats.FromQueryStats(&Ydb_TableStats.QueryStats{})

		p, ok := s.NextPhase()

		require.False(t, ok)
		require.Nil(t, p)
	})

	t.Run("ReturnsFalseOnNilPhaseEntry", func(t *testing.T) {
		// A nil entry inside the QueryPhases slice must terminate iteration
		// cleanly instead of panicking when callers later dereference the
		// returned phase.
		s := stats.FromQueryStats(queryStatsWithNilPhase())

		p, ok := s.NextPhase()

		require.False(t, ok)
		require.Nil(t, p)
	})
}

func TestQueryPhaseNextTableAccess(t *testing.T) {
	t.Run("WalksAllAccessesAndStops", func(t *testing.T) {
		s := stats.FromQueryStats(sampleQueryStatsPB())
		phase, ok := s.NextPhase()
		require.True(t, ok)

		ta, ok := phase.NextTableAccess()
		require.True(t, ok)
		require.Equal(t, &stats.TableAccess{
			Name:            "a",
			Reads:           stats.OperationStats{Rows: 100, Bytes: 200},
			Updates:         stats.OperationStats{Rows: 300, Bytes: 400},
			Deletes:         stats.OperationStats{Rows: 500, Bytes: 600},
			PartitionsCount: 700,
		}, ta)

		end, ok := phase.NextTableAccess()
		require.False(t, ok)
		require.Nil(t, end)
	})

	t.Run("ReturnsFalseOnEmpty", func(t *testing.T) {
		s := stats.FromQueryStats(queryStatsWithEmptyPhase())
		phase, ok := s.NextPhase()
		require.True(t, ok)

		ta, ok := phase.NextTableAccess()

		require.False(t, ok)
		require.Nil(t, ta)
	})
}
