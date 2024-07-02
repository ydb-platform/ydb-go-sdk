package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

func TestFromQueryStats(t *testing.T) {
	require.Equal(t, &Query{
		QueryPhases: []Phase{
			{
				Duration: time.Duration(10) * time.Microsecond,
				TableAccess: []TableAccess{
					{
						Name: "a",
						Reads: Operation{
							Rows:  100,
							Bytes: 200,
						},
						Updates: Operation{
							Rows:  300,
							Bytes: 400,
						},
						Deletes: Operation{
							Rows:  500,
							Bytes: 600,
						},
						PartitionsCount: 700,
					},
				},
				CPUTime:        time.Duration(20) * time.Microsecond,
				AffectedShards: 30,
				LiteralPhase:   true,
			},
			{
				Duration: time.Duration(11) * time.Microsecond,
				TableAccess: []TableAccess{
					{
						Name: "b",
						Reads: Operation{
							Rows:  101,
							Bytes: 201,
						},
						Updates: Operation{
							Rows:  301,
							Bytes: 401,
						},
						Deletes: Operation{
							Rows:  501,
							Bytes: 601,
						},
						PartitionsCount: 701,
					},
				},
				CPUTime:        time.Duration(21) * time.Microsecond,
				AffectedShards: 31,
				LiteralPhase:   false,
			},
		},
		Compilation: Compilation{
			FromCache: true,
			Duration:  time.Duration(123) * time.Microsecond,
			CPUTime:   time.Duration(456) * time.Microsecond,
		},
		Plan:           "123",
		Ast:            "456",
		ProcessCPUTime: time.Duration(100) * time.Microsecond,
		TotalDuration:  time.Duration(200) * time.Microsecond,
		TotalCPUTime:   time.Duration(300) * time.Microsecond,
	}, FromQueryStats(&Ydb_TableStats.QueryStats{
		QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
			{
				DurationUs: 10,
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					{
						Name: "a",
						Reads: &Ydb_TableStats.OperationStats{
							Rows:  100,
							Bytes: 200,
						},
						Updates: &Ydb_TableStats.OperationStats{
							Rows:  300,
							Bytes: 400,
						},
						Deletes: &Ydb_TableStats.OperationStats{
							Rows:  500,
							Bytes: 600,
						},
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
						Name: "b",
						Reads: &Ydb_TableStats.OperationStats{
							Rows:  101,
							Bytes: 201,
						},
						Updates: &Ydb_TableStats.OperationStats{
							Rows:  301,
							Bytes: 401,
						},
						Deletes: &Ydb_TableStats.OperationStats{
							Rows:  501,
							Bytes: 601,
						},
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
		QueryPlan:        "123",
		QueryAst:         "456",
		TotalDurationUs:  200,
		TotalCpuTimeUs:   300,
	}))
}
