package stats

import "time"

type QueryPhase interface {
	NextTableAccess() (t *TableAccess, ok bool)
	Duration() time.Duration
	CPUTime() time.Duration
	AffectedShards() uint64
}

type QueryStats interface {
	ProcessCPUTime() time.Duration
	Compilation() (c *CompilationStats)
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
