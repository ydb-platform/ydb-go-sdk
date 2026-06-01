package stats

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

func TestFromQueryStats(t *testing.T) {
	s := FromQueryStats(Ydb_TableStats.QueryStats_builder{
		QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
			Ydb_TableStats.QueryPhaseStats_builder{
				DurationUs: 10,
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					Ydb_TableStats.TableAccessStats_builder{
						Name: "a",
						Reads: Ydb_TableStats.OperationStats_builder{
							Rows:  100,
							Bytes: 200,
						}.Build(),
						Updates: Ydb_TableStats.OperationStats_builder{
							Rows:  300,
							Bytes: 400,
						}.Build(),
						Deletes: Ydb_TableStats.OperationStats_builder{
							Rows:  500,
							Bytes: 600,
						}.Build(),
						PartitionsCount: 700,
					}.Build(),
				},
				CpuTimeUs:      20,
				AffectedShards: 30,
				LiteralPhase:   true,
			}.Build(),
			Ydb_TableStats.QueryPhaseStats_builder{
				DurationUs: 11,
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					Ydb_TableStats.TableAccessStats_builder{
						Name: "b",
						Reads: Ydb_TableStats.OperationStats_builder{
							Rows:  101,
							Bytes: 201,
						}.Build(),
						Updates: Ydb_TableStats.OperationStats_builder{
							Rows:  301,
							Bytes: 401,
						}.Build(),
						Deletes: Ydb_TableStats.OperationStats_builder{
							Rows:  501,
							Bytes: 601,
						}.Build(),
						PartitionsCount: 701,
					}.Build(),
				},
				CpuTimeUs:      21,
				AffectedShards: 31,
				LiteralPhase:   false,
			}.Build(),
		},
		Compilation: Ydb_TableStats.CompilationStats_builder{
			FromCache:  true,
			DurationUs: 123,
			CpuTimeUs:  456,
		}.Build(),
		ProcessCpuTimeUs: 100,
		QueryPlan:        "plan",
		QueryAst:         "ast",
		TotalDurationUs:  200,
		TotalCpuTimeUs:   300,
	}.Build())
	require.Equal(t, fromUs(100), s.ProcessCPUTime())
	require.Equal(t, fromUs(200), s.TotalDuration())
	require.Equal(t, fromUs(300), s.TotalCPUTime())
	require.Equal(t, "ast", s.QueryAST())
	require.Equal(t, "plan", s.QueryPlan())
	require.Equal(t, &CompilationStats{
		FromCache: true,
		Duration:  fromUs(123),
		CPUTime:   fromUs(456),
	}, s.Compilation())
	phase1, ok := s.NextPhase()
	require.True(t, ok)
	require.True(t, phase1.IsLiteralPhase())
	require.Equal(t, fromUs(10), phase1.Duration())
	require.Equal(t, fromUs(20), phase1.CPUTime())
	require.Equal(t, uint64(30), phase1.AffectedShards())
	tableAccess1FromPhase1, ok := phase1.NextTableAccess()
	require.True(t, ok)
	require.Equal(t, &TableAccess{
		Name: "a",
		Reads: OperationStats{
			Rows:  100,
			Bytes: 200,
		},
		Updates: OperationStats{
			Rows:  300,
			Bytes: 400,
		},
		Deletes: OperationStats{
			Rows:  500,
			Bytes: 600,
		},
		PartitionsCount: 700,
	}, tableAccess1FromPhase1)
	tableAccess2FromPhase1, ok := phase1.NextTableAccess()
	require.False(t, ok)
	require.Nil(t, tableAccess2FromPhase1)
	phase2, ok := s.NextPhase()
	require.True(t, ok)
	require.False(t, phase2.IsLiteralPhase())
	require.Equal(t, fromUs(11), phase2.Duration())
	require.Equal(t, fromUs(21), phase2.CPUTime())
	require.Equal(t, uint64(31), phase2.AffectedShards())
	tableAccess1FromPhase2, ok := phase2.NextTableAccess()
	require.True(t, ok)
	require.Equal(t, &TableAccess{
		Name: "b",
		Reads: OperationStats{
			Rows:  101,
			Bytes: 201,
		},
		Updates: OperationStats{
			Rows:  301,
			Bytes: 401,
		},
		Deletes: OperationStats{
			Rows:  501,
			Bytes: 601,
		},
		PartitionsCount: 701,
	}, tableAccess1FromPhase2)
	tableAccess2FromPhase2, ok := phase2.NextTableAccess()
	require.False(t, ok)
	require.Nil(t, tableAccess2FromPhase2)
}
