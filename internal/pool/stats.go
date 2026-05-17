package pool

type Stats struct {
	Limit            int
	WarmUp           int
	Size             int
	Idle             int
	CreateInProgress int
	Concurrency      int
	InUse            int
}
