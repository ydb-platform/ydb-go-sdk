package pool

type (
	dynamicStats struct {
		Size             int
		Idle             int
		CreateInProgress int
		Concurrency      int
		InUse            int
	}
	Stats struct {
		dynamicStats

		Limit  int
		WarmUp int
	}
)
