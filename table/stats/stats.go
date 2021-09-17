package stats

import (
	"time"
)

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
