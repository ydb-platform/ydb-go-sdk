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

// sampleQueryStatsPB returns a fully-populated QueryStats proto used by
// scalar-field tests. Two query phases let us also exercise NextPhase
// advancement and table access on more than one phase.
func sampleQueryStatsPB() *Ydb_TableStats.QueryStats {
	return &Ydb_TableStats.QueryStats{
		QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
			{
				DurationUs: 10,
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					{
						Name:            "a",
						Reads:           &Ydb_TableStats.OperationStats{Rows: 100, Bytes: 200},
						Updates:         &Ydb_TableStats.OperationStats{Rows: 300, Bytes: 400},
						Deletes:         &Ydb_TableStats.OperationStats{Rows: 500, Bytes: 600},
						PartitionsCount: 700,
					},
				},
				CpuTimeUs:      20,
				AffectedShards: 30,
				LiteralPhase:   true,
			},
			{
				DurationUs: 11,
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					{
						Name:            "b",
						Reads:           &Ydb_TableStats.OperationStats{Rows: 101, Bytes: 201},
						Updates:         &Ydb_TableStats.OperationStats{Rows: 301, Bytes: 401},
						Deletes:         &Ydb_TableStats.OperationStats{Rows: 501, Bytes: 601},
						PartitionsCount: 701,
					},
				},
				CpuTimeUs:      21,
				AffectedShards: 31,
				LiteralPhase:   false,
			},
		},
		Compilation: &Ydb_TableStats.CompilationStats{
			FromCache:  true,
			DurationUs: 123,
			CpuTimeUs:  456,
		},
		ProcessCpuTimeUs: 100,
		QueryPlan:        "plan",
		QueryAst:         "ast",
		TotalDurationUs:  200,
		TotalCpuTimeUs:   300,
	}
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
		s := stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{nil},
		})

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
		s := stats.FromQueryStats(&Ydb_TableStats.QueryStats{
			QueryPhases: []*Ydb_TableStats.QueryPhaseStats{{}},
		})
		phase, ok := s.NextPhase()
		require.True(t, ok)

		ta, ok := phase.NextTableAccess()

		require.False(t, ok)
		require.Nil(t, ta)
	})
}
