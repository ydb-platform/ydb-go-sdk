package metrics

import "time"

type Job struct {
	name  JobName
	start time.Time
	m     *Metrics
}

type JobName = string

const (
	JobRead  JobName = "read"
	JobWrite JobName = "write"
)
