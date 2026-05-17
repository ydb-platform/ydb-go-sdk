package pool

type Stats struct {
	Limit            int
	Idle             int
	CreateInProgress int
	Concurrency      int
}
