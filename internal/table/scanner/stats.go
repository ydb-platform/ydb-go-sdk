package scanner

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/stats"
)

// queryStats holds query execution statistics.
type queryStats struct {
	stats          *Ydb_TableStats.QueryStats
	processCPUTime time.Duration
	pos            int
}

func (s *queryStats) ProcessCPUTime() time.Duration {
	return s.processCPUTime
}

func (s *queryStats) Compilation() (c *stats.CompilationStats) {
	if s.stats == nil || s.stats.GetCompilation() == nil {
		return nil
	}

	return &stats.CompilationStats{
		FromCache: s.stats.GetCompilation().GetFromCache(),
		Duration:  time.Microsecond * time.Duration(s.stats.GetCompilation().GetDurationUs()),
		CPUTime:   time.Microsecond * time.Duration(s.stats.GetCompilation().GetCpuTimeUs()),
	}
}

func (s *queryStats) QueryPlan() string {
	return s.stats.GetQueryPlan()
}

func (s *queryStats) QueryAST() string {
	return s.stats.GetQueryAst()
}

func (s *queryStats) TotalCPUTime() time.Duration {
	return time.Microsecond * time.Duration(s.stats.GetTotalCpuTimeUs())
}

func (s *queryStats) TotalDuration() time.Duration {
	return time.Microsecond * time.Duration(s.stats.GetTotalDurationUs())
}

// NextPhase returns next execution phase within query.
// If ok flag is false, then there are no more phases and p is invalid.
func (s *queryStats) NextPhase() (p stats.QueryPhase, ok bool) {
	if s.pos >= len(s.stats.GetQueryPhases()) {
		return
	}
	x := s.stats.GetQueryPhases()[s.pos]
	if x == nil {
		return
	}
	s.pos++

	return &queryPhase{
		tables:         x.GetTableAccess(),
		pos:            0,
		duration:       time.Microsecond * time.Duration(x.GetDurationUs()),
		cpuTime:        time.Microsecond * time.Duration(x.GetCpuTimeUs()),
		affectedShards: x.GetAffectedShards(),
		literalPhase:   x.GetLiteralPhase(),
	}, true
}

// queryPhase holds query execution phase statistics.
type queryPhase struct {
	duration       time.Duration
	cpuTime        time.Duration
	affectedShards uint64
	tables         []*Ydb_TableStats.TableAccessStats
	pos            int
	literalPhase   bool
}

// NextTableAccess returns next accessed table within query execution phase.
//
// If ok flag is false, then there are no more accessed tables and t is
// invalid.
func (q *queryPhase) NextTableAccess() (t *stats.TableAccess, ok bool) {
	if q.pos >= len(q.tables) {
		return
	}
	x := q.tables[q.pos]
	q.pos++

	return &stats.TableAccess{
		Name:    x.GetName(),
		Reads:   initOperationStats(x.GetReads()),
		Updates: initOperationStats(x.GetUpdates()),
		Deletes: initOperationStats(x.GetDeletes()),
	}, true
}

func (q *queryPhase) Duration() time.Duration {
	return q.duration
}

func (q *queryPhase) CPUTime() time.Duration {
	return q.cpuTime
}

func (q *queryPhase) AffectedShards() uint64 {
	return q.affectedShards
}

func (q *queryPhase) IsLiteralPhase() bool {
	return q.literalPhase
}

func initOperationStats(x *Ydb_TableStats.OperationStats) stats.OperationStats {
	if x == nil {
		return stats.OperationStats{
			Rows:  0,
			Bytes: 0,
		}
	}

	return stats.OperationStats{
		Rows:  x.GetRows(),
		Bytes: x.GetBytes(),
	}
}
