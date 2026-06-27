package options

type ConcurrentResultSets bool

//nolint:revive
const (
	CONCURRENT_RESULT_SETS = ConcurrentResultSets(true)
	ORDERED_RESULT_SETS    = ConcurrentResultSets(false)
)
