package table

import (
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_TableStats"
)

// QueryStats holds query execution statistics.
type QueryStats struct {
	stats *Ydb_TableStats.QueryStats
	pos   int
}

// NextPhase returns next execution phase within query.
// If ok flag is false, then there are no more phases and p is invalid.
func (s *QueryStats) NextPhase() (p QueryPhase, ok bool) {
	if s.stats == nil || s.pos >= len(s.stats.QueryPhases) {
		return
	}
	p.init(s.stats.QueryPhases[s.pos])
	s.pos++
	return p, true
}

// QueryPhase holds query execution phase statistics.
type QueryPhase struct {
	Duration time.Duration

	tables []*Ydb_TableStats.TableAccessStats
	pos    int
}

func (q *QueryPhase) init(x *Ydb_TableStats.QueryPhaseStats) {
	if x == nil {
		return
	}
	q.Duration = time.Microsecond * time.Duration(x.DurationUs)
	q.tables = x.TableAccess
}

// NextTableAccess returns next accessed table within query execution phase.
//
// If ok flag is false, then there are no more accessed tables and t is
// invalid.
func (q *QueryPhase) NextTableAccess() (t TableAccess, ok bool) {
	if q.pos >= len(q.tables) {
		return
	}
	t.init(q.tables[q.pos])
	q.pos++
	return t, true
}

// TableAccess contains query execution phase's table access statistics.
type TableAccess struct {
	Name    string
	Reads   OperationStats
	Updates OperationStats
	Deletes OperationStats
}

func (t *TableAccess) init(x *Ydb_TableStats.TableAccessStats) {
	if x == nil {
		return
	}
	t.Name = x.Name
	t.Reads.init(x.Reads)
	t.Updates.init(x.Updates)
	t.Deletes.init(x.Deletes)
}

type OperationStats struct {
	Rows  uint64
	Bytes uint64
}

func (s *OperationStats) init(x *Ydb_TableStats.OperationStats) {
	if x == nil {
		return
	}
	*s = OperationStats{
		Rows:  x.Rows,
		Bytes: x.Bytes,
	}
}
