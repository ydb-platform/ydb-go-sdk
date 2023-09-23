package stats

import "time"

// QueryPhase holds query execution phase statistics.
type QueryPhase interface {
	// NextTableAccess returns next accessed table within query execution phase.
	// If ok flag is false, then there are no more accessed tables and t is invalid.
	NextTableAccess() (t *TableAccess, ok bool)
	Duration() time.Duration
	CPUTime() time.Duration
	AffectedShards() uint64
	IsLiteralPhase() bool
}

// QueryStats holds query execution statistics.
type QueryStats interface {
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

// CompilationStats holds query compilation statistics.
type CompilationStats struct {
	FromCache bool
	Duration  time.Duration
	CPUTime   time.Duration
}

// TableAccess contains query execution phase's table access statistics.
type TableAccess struct {
	Name    string
	Reads   OperationStats
	Updates OperationStats
	Deletes OperationStats
}

type OperationStats struct {
	Rows  uint64
	Bytes uint64
}
