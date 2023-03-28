package metrics

import "time"

type job struct {
	name  JobName
	start time.Time
}

type JobName = string

const (
	JobRead  JobName = "read"
	JobWrite JobName = "write"
)
