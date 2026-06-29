package options

type ConcurrentResultSetsType struct {
	Flag bool
}

var (
	ConcurrentResultSets = ConcurrentResultSetsType{true}
	OrderedResultSets    = ConcurrentResultSetsType{false}
)
