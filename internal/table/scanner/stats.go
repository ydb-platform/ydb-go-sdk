package scanner

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/stats"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

// QueryStats holds query execution statistics.
type QueryStats struct {
	stats          *Ydb_TableStats.QueryStats
	processCPUTime time.Duration
	pos            int
}

func (s *QueryStats) ProcessCPUTime() time.Duration {
	if s == nil {
		return 0
	}
	return s.processCPUTime
}

func (s *QueryStats) Compilation() (c *stats.CompilationStats) {
	if s == nil || s.stats == nil || s.stats.Compilation == nil {
		return nil
	}
	x := s.stats.Compilation
	if x == nil {
		return
	}
	return &stats.CompilationStats{
		FromCache: x.FromCache,
		Duration:  time.Microsecond * time.Duration(x.DurationUs),
		CPUTime:   time.Microsecond * time.Duration(x.CpuTimeUs),
	}
}

// NextPhase returns next execution phase within query.
// If ok flag is false, then there are no more phases and p is invalid.
func (s *QueryStats) NextPhase() (p QueryPhase, ok bool) {
	if s.stats == nil || s.pos >= len(s.stats.QueryPhases) {
		return
	}
	x := s.stats.QueryPhases[s.pos]
	if x == nil {
		return
	}
	s.pos++
	return QueryPhase{
		tables:         x.TableAccess,
		pos:            0,
		Duration:       time.Microsecond * time.Duration(x.DurationUs),
		CPUTime:        time.Microsecond * time.Duration(x.CpuTimeUs),
		AffectedShards: x.AffectedShards,
	}, true
}

// QueryPhase holds query execution phase statistics.
type QueryPhase struct {
	Duration       time.Duration
	CPUTime        time.Duration
	AffectedShards uint64
	tables         []*Ydb_TableStats.TableAccessStats
	pos            int
}

// NextTableAccess returns next accessed table within query execution phase.
//
// If ok flag is false, then there are no more accessed tables and t is
// invalid.
func (q *QueryPhase) NextTableAccess() (t stats.TableAccess, ok bool) {
	if q.pos >= len(q.tables) {
		return
	}
	x := q.tables[q.pos]
	q.pos++
	return stats.TableAccess{
		Name:    x.Name,
		Reads:   initOperationStats(x.Reads),
		Updates: initOperationStats(x.Updates),
		Deletes: initOperationStats(x.Deletes),
	}, true
}

func initOperationStats(x *Ydb_TableStats.OperationStats) stats.OperationStats {
	if x == nil {
		return stats.OperationStats{}
	}
	return stats.OperationStats{
		Rows:  x.Rows,
		Bytes: x.Bytes,
	}
}
