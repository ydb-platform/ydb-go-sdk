package options

type ConcurrentResultSets struct {
	Flag bool
}

//nolint:revive
var (
	CONCURRENT_RESULT_SETS = ConcurrentResultSets{true}
	ORDERED_RESULT_SETS    = ConcurrentResultSets{false}
)
