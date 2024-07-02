package stats

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

type (
	Operation struct {
		Rows  uint64
		Bytes uint64
	}
	TableAccess struct {
		Name            string
		Reads           Operation
		Updates         Operation
		Deletes         Operation
		PartitionsCount uint64
	}
	Phase struct {
		Duration       time.Duration
		TableAccess    []TableAccess
		CPUTime        time.Duration
		AffectedShards uint64
		LiteralPhase   bool
	}
	Compilation struct {
		FromCache bool
		Duration  time.Duration
		CPUTime   time.Duration
	}
	Query struct {
		QueryPhases    []Phase
		Compilation    Compilation
		Plan           string
		Ast            string
		ProcessCPUTime time.Duration
		TotalDuration  time.Duration
		TotalCPUTime   time.Duration
	}
)

func fromUs(us uint64) time.Duration {
	return time.Duration(us) * time.Microsecond
}

func fromCompilationStats(pb *Ydb_TableStats.CompilationStats) Compilation {
	return Compilation{
		FromCache: pb.GetFromCache(),
		Duration:  fromUs(pb.GetDurationUs()),
		CPUTime:   fromUs(pb.GetCpuTimeUs()),
	}
}

func fromQueryPhases(pb []*Ydb_TableStats.QueryPhaseStats) (phases []Phase) {
	phases = make([]Phase, len(pb))
	for i := range pb {
		phases[i] = fromQueryPhaseStats(pb[i])
	}

	return phases
}

func fromQueryPhaseStats(pb *Ydb_TableStats.QueryPhaseStats) Phase {
	return Phase{
		Duration:       fromUs(pb.GetDurationUs()),
		TableAccess:    fromTableAccess(pb.GetTableAccess()),
		CPUTime:        fromUs(pb.GetCpuTimeUs()),
		AffectedShards: pb.GetAffectedShards(),
		LiteralPhase:   pb.GetLiteralPhase(),
	}
}

func fromTableAccess(pb []*Ydb_TableStats.TableAccessStats) (tableAccess []TableAccess) {
	tableAccess = make([]TableAccess, len(pb))
	for i := range pb {
		tableAccess[i] = fromTableAccessStats(pb[i])
	}

	return tableAccess
}

func fromTableAccessStats(pb *Ydb_TableStats.TableAccessStats) TableAccess {
	return TableAccess{
		Name:            pb.GetName(),
		Reads:           fromOperationStats(pb.GetReads()),
		Updates:         fromOperationStats(pb.GetUpdates()),
		Deletes:         fromOperationStats(pb.GetDeletes()),
		PartitionsCount: pb.GetPartitionsCount(),
	}
}

func fromOperationStats(pb *Ydb_TableStats.OperationStats) Operation {
	return Operation{
		Rows:  pb.GetRows(),
		Bytes: pb.GetBytes(),
	}
}

func FromQueryStats(pb *Ydb_TableStats.QueryStats) *Query {
	if pb == nil {
		return nil
	}

	return &Query{
		QueryPhases:    fromQueryPhases(pb.GetQueryPhases()),
		Compilation:    fromCompilationStats(pb.GetCompilation()),
		Plan:           pb.GetQueryPlan(),
		Ast:            pb.GetQueryAst(),
		ProcessCPUTime: fromUs(pb.GetProcessCpuTimeUs()),
		TotalDuration:  fromUs(pb.GetTotalDurationUs()),
		TotalCPUTime:   fromUs(pb.GetTotalCpuTimeUs()),
	}
}
