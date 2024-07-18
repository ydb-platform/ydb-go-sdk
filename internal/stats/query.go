package stats

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

type (
	// QueryStats holds query execution statistics.
	QueryStats interface {
		ProcessCPUTime() time.Duration
		Compilation() (c *CompilationStats)
		QueryPlan() string
		QueryAST() string
		TotalCPUTime() time.Duration
		TotalDuration() time.Duration

		// NextPhase returns next execution phase within query.
		// If ok flag is false, then there are no more phases and p is invalid.
		NextPhase() (p QueryPhase, ok bool)
	}
	// QueryPhase holds query execution phase statistics.
	QueryPhase interface {
		// NextTableAccess returns next accessed table within query execution phase.
		// If ok flag is false, then there are no more accessed tables and t is invalid.
		NextTableAccess() (t *TableAccess, ok bool)
		Duration() time.Duration
		CPUTime() time.Duration
		AffectedShards() uint64
		IsLiteralPhase() bool
	}
	OperationStats struct {
		Rows  uint64
		Bytes uint64
	}
	Phase struct {
		Duration       time.Duration
		TableAccess    []TableAccess
		CPUTime        time.Duration
		AffectedShards uint64
		LiteralPhase   bool
	}
	// TableAccess contains query execution phase's table access statistics.
	TableAccess struct {
		Name            string
		Reads           OperationStats
		Updates         OperationStats
		Deletes         OperationStats
		PartitionsCount uint64
	}
	// CompilationStats holds query compilation statistics.
	CompilationStats struct {
		FromCache bool
		Duration  time.Duration
		CPUTime   time.Duration
	}
	// queryStats holds query execution statistics.
	queryStats struct {
		pb  *Ydb_TableStats.QueryStats
		pos int
	}
	// queryPhase holds query execution phase statistics.
	queryPhase struct {
		pb  *Ydb_TableStats.QueryPhaseStats
		pos int
	}
)

func fromUs(us uint64) time.Duration {
	return time.Duration(us) * time.Microsecond
}

func fromCompilationStats(pb *Ydb_TableStats.CompilationStats) *CompilationStats {
	return &CompilationStats{
		FromCache: pb.GetFromCache(),
		Duration:  fromUs(pb.GetDurationUs()),
		CPUTime:   fromUs(pb.GetCpuTimeUs()),
	}
}

func fromOperationStats(pb *Ydb_TableStats.OperationStats) OperationStats {
	return OperationStats{
		Rows:  pb.GetRows(),
		Bytes: pb.GetBytes(),
	}
}

func (s *queryStats) ProcessCPUTime() time.Duration {
	return fromUs(s.pb.GetProcessCpuTimeUs())
}

func (s *queryStats) Compilation() (c *CompilationStats) {
	return fromCompilationStats(s.pb.GetCompilation())
}

func (s *queryStats) QueryPlan() string {
	return s.pb.GetQueryPlan()
}

func (s *queryStats) QueryAST() string {
	return s.pb.GetQueryAst()
}

func (s *queryStats) TotalCPUTime() time.Duration {
	return fromUs(s.pb.GetTotalCpuTimeUs())
}

func (s *queryStats) TotalDuration() time.Duration {
	return fromUs(s.pb.GetTotalDurationUs())
}

// NextPhase returns next execution phase within query.
// If ok flag is false, then there are no more phases and p is invalid.
func (s *queryStats) NextPhase() (p QueryPhase, ok bool) {
	if s.pos >= len(s.pb.GetQueryPhases()) {
		return
	}
	pb := s.pb.GetQueryPhases()[s.pos]
	if pb == nil {
		return
	}
	s.pos++

	return &queryPhase{
		pb: pb,
	}, true
}

// NextTableAccess returns next accessed table within query execution phase.
//
// If ok flag is false, then there are no more accessed tables and t is
// invalid.
func (queryPhase *queryPhase) NextTableAccess() (t *TableAccess, ok bool) {
	if queryPhase.pos >= len(queryPhase.pb.GetTableAccess()) {
		return
	}
	pb := queryPhase.pb.GetTableAccess()[queryPhase.pos]
	queryPhase.pos++

	return &TableAccess{
		Name:            pb.GetName(),
		Reads:           fromOperationStats(pb.GetReads()),
		Updates:         fromOperationStats(pb.GetUpdates()),
		Deletes:         fromOperationStats(pb.GetDeletes()),
		PartitionsCount: pb.GetPartitionsCount(),
	}, true
}

func (queryPhase *queryPhase) Duration() time.Duration {
	return fromUs(queryPhase.pb.GetDurationUs())
}

func (queryPhase *queryPhase) CPUTime() time.Duration {
	return fromUs(queryPhase.pb.GetCpuTimeUs())
}

func (queryPhase *queryPhase) AffectedShards() uint64 {
	return queryPhase.pb.GetAffectedShards()
}

func (queryPhase *queryPhase) IsLiteralPhase() bool {
	return queryPhase.pb.GetLiteralPhase()
}

func FromQueryStats(pb *Ydb_TableStats.QueryStats) QueryStats {
	if pb == nil {
		return nil
	}

	return &queryStats{
		pb: pb,
	}
}
