package timetest

import (
	"time"
)

type Timer struct {
	Ch      <-chan time.Time
	OnReset func(time.Duration) bool
	OnStop  func() bool
}

func (t Timer) Reset(d time.Duration) bool {
	if t.OnReset != nil {
		return t.OnReset(d)
	}
	return true
}

func (t Timer) Stop() bool {
	if t.OnStop != nil {
		return t.OnStop()
	}
	return true
}

func (t Timer) C() <-chan time.Time {
	return t.Ch
}
