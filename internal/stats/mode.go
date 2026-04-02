package stats

// Mode represents statistics collection mode.
//
// The enumeration values correspond to [github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query.StatsMode].
type Mode int32

const (
	ModeUnspecified Mode = 0
	ModeNode        Mode = 10 // Stats collection is disabled
	ModeBasic       Mode = 20 // Aggregated stats of reads, updates and deletes per table
	ModeFull        Mode = 30 // Add execution stats and plan on top of [ModeBasic]
	ModeProfile     Mode = 40 // Detailed execution stats including stats for individual tasks and channels
)
