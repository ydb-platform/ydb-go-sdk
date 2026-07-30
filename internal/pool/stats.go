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

func (s *dynamicStats) add(other dynamicStats) {
	s.Size += other.Size
	s.Idle += other.Idle
	s.CreateInProgress += other.CreateInProgress
	s.Concurrency += other.Concurrency
	s.InUse += other.InUse
}
