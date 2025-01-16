package stats

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
)

type (
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
	// QueryStats holds query execution statistics.
	QueryStats struct {
		pb  *Ydb_TableStats.QueryStats
		pos int
	}
	// QueryPhase holds query execution phase statistics.
	QueryPhase struct {
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

func (s *QueryStats) ProcessCPUTime() time.Duration {
	return fromUs(s.pb.GetProcessCpuTimeUs())
}

func (s *QueryStats) Compilation() (c *CompilationStats) {
	return fromCompilationStats(s.pb.GetCompilation())
}

func (s *QueryStats) QueryPlan() string {
	return s.pb.GetQueryPlan()
}

func (s *QueryStats) QueryAST() string {
	return s.pb.GetQueryAst()
}

func (s *QueryStats) TotalCPUTime() time.Duration {
	return fromUs(s.pb.GetTotalCpuTimeUs())
}

func (s *QueryStats) TotalDuration() time.Duration {
	return fromUs(s.pb.GetTotalDurationUs())
}

// NextPhase returns next execution phase within query.
// If ok flag is false, then there are no more phases and p is invalid.
func (s *QueryStats) NextPhase() (p QueryPhase, ok bool) {
	if s.pos >= len(s.pb.GetQueryPhases()) {
		return
	}
	pb := s.pb.GetQueryPhases()[s.pos]
	if pb == nil {
		return
	}
	s.pos++

	return QueryPhase{
		pb: pb,
	}, true
}

func (s *QueryStats) RangeQueryPhases() xiter.Seq[QueryPhase] {
	return func(yield func(p QueryPhase) bool) {
		for _, pb := range s.pb.GetQueryPhases() {
			cont := yield(QueryPhase{
				pb: pb,
			})
			if !cont {
				return
			}
		}
	}
}

// NextTableAccess returns next accessed table within query execution phase.
//
// If ok flag is false, then there are no more accessed tables and t is
// invalid.
func (phase *QueryPhase) NextTableAccess() (t *TableAccess, ok bool) {
	if phase.pos >= len(phase.pb.GetTableAccess()) {
		return
	}
	pb := phase.pb.GetTableAccess()[phase.pos]
	phase.pos++

	return &TableAccess{
		Name:            pb.GetName(),
		Reads:           fromOperationStats(pb.GetReads()),
		Updates:         fromOperationStats(pb.GetUpdates()),
		Deletes:         fromOperationStats(pb.GetDeletes()),
		PartitionsCount: pb.GetPartitionsCount(),
	}, true
}

func (phase *QueryPhase) RangeTableAccess() xiter.Seq[*TableAccess] {
	return func(yield func(access *TableAccess) bool) {
		for _, pb := range phase.pb.GetTableAccess() {
			cont := yield(&TableAccess{
				Name:            pb.GetName(),
				Reads:           fromOperationStats(pb.GetReads()),
				Updates:         fromOperationStats(pb.GetUpdates()),
				Deletes:         fromOperationStats(pb.GetDeletes()),
				PartitionsCount: pb.GetPartitionsCount(),
			})
			if !cont {
				return
			}
		}
	}
}

func (phase *QueryPhase) Duration() time.Duration {
	return fromUs(phase.pb.GetDurationUs())
}

func (phase *QueryPhase) CPUTime() time.Duration {
	return fromUs(phase.pb.GetCpuTimeUs())
}

func (phase *QueryPhase) AffectedShards() uint64 {
	return phase.pb.GetAffectedShards()
}

func (phase *QueryPhase) IsLiteralPhase() bool {
	return phase.pb.GetLiteralPhase()
}

func FromQueryStats(pb *Ydb_TableStats.QueryStats) *QueryStats {
	if pb == nil {
		return nil
	}

	return &QueryStats{
		pb: pb,
	}
}
